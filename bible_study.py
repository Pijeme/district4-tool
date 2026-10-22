"""Bible Study: NIV reader, independent Drive sync and source-specific dictionaries.

Adapted from Bible_Study_Codex_Starter. EPUBs are offline indexing sources only.
Database creation requires the explicit maintenance CLI or administrator sync;
ordinary reading requests never create or migrate a database.
"""
import argparse
import copy
import ctypes
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from functools import lru_cache
from html import unescape as unescape_html
import hmac
import hashlib
import gzip
import html.entities
import io
import json
import logging
import os
from pathlib import Path
import posixpath
import re
import secrets
import sqlite3
import threading
import tempfile
import time
import unicodedata
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET
import zipfile
import zlib

from flask import Blueprint, Response, current_app, jsonify, redirect, render_template, request, session, url_for
from itsdangerous import URLSafeSerializer, BadSignature

import pastor_resources as resources
from pastor_resources import current_reader_user_key, is_resource_admin


BASE_DIR = Path(__file__).resolve().parent
SCHEMA_VERSION = 7
APPLICATION_ID = 0x4249424C
MAX_JSON_BYTES = 16384
STUDY_SAVE_SCHEMA_SQL = '''CREATE TABLE IF NOT EXISTS bible_study_saves (
    id TEXT PRIMARY KEY,user_key TEXT NOT NULL,group_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('highlight','underline','note','bookmark')),
    source_key TEXT NOT NULL,source_json TEXT NOT NULL,block_text TEXT NOT NULL,
    start_offset INTEGER NOT NULL,end_offset INTEGER NOT NULL,selected_text TEXT NOT NULL,
    color TEXT,note TEXT NOT NULL DEFAULT '',version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS bible_study_save_owner ON bible_study_saves(user_key,source_key,id);'''
BOOKS = (
    ('Genesis', 50), ('Exodus', 40), ('Leviticus', 27), ('Numbers', 36), ('Deuteronomy', 34),
    ('Joshua', 24), ('Judges', 21), ('Ruth', 4), ('1 Samuel', 31), ('2 Samuel', 24),
    ('1 Kings', 22), ('2 Kings', 25), ('1 Chronicles', 29), ('2 Chronicles', 36), ('Ezra', 10),
    ('Nehemiah', 13), ('Esther', 10), ('Job', 42), ('Psalms', 150), ('Proverbs', 31),
    ('Ecclesiastes', 12), ('Song of Songs', 8), ('Isaiah', 66), ('Jeremiah', 52), ('Lamentations', 5),
    ('Ezekiel', 48), ('Daniel', 12), ('Hosea', 14), ('Joel', 3), ('Amos', 9), ('Obadiah', 1),
    ('Jonah', 4), ('Micah', 7), ('Nahum', 3), ('Habakkuk', 3), ('Zephaniah', 3), ('Haggai', 2),
    ('Zechariah', 14), ('Malachi', 4), ('Matthew', 28), ('Mark', 16), ('Luke', 24), ('John', 21),
    ('Acts', 28), ('Romans', 16), ('1 Corinthians', 16), ('2 Corinthians', 13), ('Galatians', 6),
    ('Ephesians', 6), ('Philippians', 4), ('Colossians', 4), ('1 Thessalonians', 5),
    ('2 Thessalonians', 3), ('1 Timothy', 6), ('2 Timothy', 4), ('Titus', 3), ('Philemon', 1),
    ('Hebrews', 13), ('James', 5), ('1 Peter', 5), ('2 Peter', 3), ('1 John', 5), ('2 John', 1),
    ('3 John', 1), ('Jude', 1), ('Revelation', 22),
)
BOOK_BY_NAME = {name.casefold(): (index, name, count) for index, (name, count) in enumerate(BOOKS, 1)}
BOOK_BY_NAME['song of solomon'] = BOOK_BY_NAME['song of songs']
USFM_BOOK_IDS = 'GEN EXO LEV NUM DEU JOS JDG RUT 1SA 2SA 1KI 2KI 1CH 2CH EZR NEH EST JOB PSA PRO ECC SNG ISA JER LAM EZK DAN HOS JOL AMO OBA JON MIC NAM HAB ZEP HAG ZEC MAL MAT MRK LUK JHN ACT ROM 1CO 2CO GAL EPH PHP COL 1TH 2TH 1TI 2TI TIT PHM HEB JAS 1PE 2PE 1JN 2JN 3JN JUD REV'.split()
READER_TRANSLATIONS = {
    'NIV': dict(label='NIV', title='New International Version'),
    'CEBOCB': dict(label='Cebuano · Ang Pulong sa Dios', title='Biblica® Open Cebuano Contemporary Bible 2024',
        attribution='Biblica® Open Ang Pulong sa Dios™ · Copyright © 2009, 2010, 2014, 2024 Biblica, Inc. · CC BY-SA 4.0 · biblica.com / open.bible'),
    'TCB': dict(label='Tagalog · Ang Salita ng Diyos', title='Biblica® Open Tagalog Contemporary Bible 2025',
        attribution='Biblica® Open Ang Salita ng Diyos · Copyright © 2009, 2011, 2014, 2025 Biblica, Inc. · Source: Biblica® Open Tagalog Contemporary Bible 2025'),
}

# All verse/token/annotation offsets are Unicode code-point offsets into an
# immutable verse revision. Future DOM code must convert JavaScript UTF-16
# offsets explicitly. Preserve old revisions when reindexing personal data.
SCHEMA_SQL = """
CREATE TABLE bible_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE bible_books (
    id INTEGER PRIMARY KEY CHECK(id BETWEEN 1 AND 66), name TEXT NOT NULL UNIQUE,
    chapter_count INTEGER NOT NULL CHECK(chapter_count BETWEEN 1 AND 150));
CREATE TABLE bible_sources (
    id INTEGER PRIMARY KEY, title TEXT NOT NULL, edition TEXT NOT NULL DEFAULT '',
    sha256 TEXT NOT NULL UNIQUE CHECK(length(sha256)=64), source_path TEXT NOT NULL,
    attribution TEXT NOT NULL DEFAULT '', rights_note TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL);
CREATE TABLE bible_import_runs (
    id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES bible_sources(id),
    status TEXT NOT NULL CHECK(status IN ('staged','validated','failed')),
    started_at TEXT NOT NULL, finished_at TEXT, report_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE bible_editions (
    id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES bible_sources(id),
    import_run_id INTEGER NOT NULL REFERENCES bible_import_runs(id), edition_key TEXT NOT NULL UNIQUE,
    translation TEXT NOT NULL CHECK(translation IN ('NIV','KJV','CEBOCB','TCB')), title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'staged' CHECK(status IN ('staged','ready')),
    is_active INTEGER NOT NULL DEFAULT 0 CHECK(is_active IN (0,1)), attribution TEXT NOT NULL DEFAULT '');
CREATE UNIQUE INDEX bible_active_translation ON bible_editions(translation) WHERE is_active=1;
CREATE TABLE bible_verses (
    id INTEGER PRIMARY KEY, edition_id INTEGER NOT NULL REFERENCES bible_editions(id),
    book_id INTEGER NOT NULL REFERENCES bible_books(id), chapter INTEGER NOT NULL CHECK(chapter BETWEEN 1 AND 150),
    verse INTEGER NOT NULL CHECK(verse BETWEEN 1 AND 176), text TEXT NOT NULL CHECK(length(text)>0),
    heading TEXT NOT NULL DEFAULT '', content_hash TEXT NOT NULL CHECK(length(content_hash)=64),
    verse_end INTEGER NOT NULL DEFAULT 0, notes_json TEXT NOT NULL DEFAULT '[]', layout TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0), is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
    UNIQUE(edition_id,book_id,chapter,verse,revision));
CREATE UNIQUE INDEX bible_active_verse ON bible_verses(edition_id,book_id,chapter,verse) WHERE is_active=1;
CREATE TABLE bible_tokens (
    id INTEGER PRIMARY KEY, verse_id INTEGER NOT NULL REFERENCES bible_verses(id),
    token_index INTEGER NOT NULL CHECK(token_index>=0), start_offset INTEGER NOT NULL CHECK(start_offset>=0),
    end_offset INTEGER NOT NULL CHECK(end_offset>start_offset), text TEXT NOT NULL,
    normalized_text TEXT NOT NULL DEFAULT '',
    UNIQUE(verse_id,token_index));
CREATE INDEX bible_token_word ON bible_tokens(normalized_text,verse_id);
CREATE TABLE bible_lexicon (
    id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES bible_sources(id), strong_id TEXT NOT NULL,
    language TEXT NOT NULL CHECK(language IN ('Hebrew','Greek')), lemma TEXT NOT NULL DEFAULT '',
    transliteration TEXT NOT NULL DEFAULT '', pronunciation TEXT NOT NULL DEFAULT '', definition TEXT NOT NULL,
    source_anchor TEXT NOT NULL CHECK(length(trim(source_anchor))>0), UNIQUE(source_id,strong_id));
CREATE TABLE bible_word_links (
    token_id INTEGER NOT NULL REFERENCES bible_tokens(id), lexicon_id INTEGER NOT NULL REFERENCES bible_lexicon(id),
    result_type TEXT NOT NULL CHECK(result_type IN ('verified','kjv_based_lookup')),
    verified INTEGER NOT NULL DEFAULT 0 CHECK(verified IN (0,1)),
    match_method TEXT NOT NULL, evidence_source_id INTEGER NOT NULL REFERENCES bible_sources(id),
    evidence_verse_id INTEGER REFERENCES bible_verses(id), source_word TEXT NOT NULL DEFAULT '',
    evidence_locator TEXT NOT NULL CHECK(length(trim(evidence_locator))>0), evidence_detail TEXT NOT NULL,
    PRIMARY KEY(token_id,lexicon_id,result_type),
    CHECK((result_type='verified' AND verified=1) OR (result_type='kjv_based_lookup' AND verified=0)),
    CHECK(result_type!='verified' OR match_method IN ('source_word_alignment','curated_verified')),
    CHECK(result_type!='kjv_based_lookup' OR evidence_verse_id IS NOT NULL));
CREATE TABLE bible_concordance (
    id INTEGER PRIMARY KEY, lexicon_id INTEGER NOT NULL REFERENCES bible_lexicon(id),
    source_id INTEGER NOT NULL REFERENCES bible_sources(id), translation TEXT NOT NULL DEFAULT 'KJV' CHECK(translation='KJV'),
    book_id INTEGER NOT NULL REFERENCES bible_books(id), chapter INTEGER NOT NULL CHECK(chapter BETWEEN 1 AND 150),
    verse INTEGER NOT NULL CHECK(verse BETWEEN 1 AND 176), source_word TEXT NOT NULL DEFAULT '',
    source_anchor TEXT NOT NULL CHECK(length(trim(source_anchor))>0),
    UNIQUE(lexicon_id,source_id,book_id,chapter,verse,source_word));
CREATE INDEX bible_concordance_reference ON bible_concordance(lexicon_id,book_id,chapter,verse,id);
CREATE TABLE bible_red_letter_spans (
    id INTEGER PRIMARY KEY, verse_id INTEGER NOT NULL REFERENCES bible_verses(id),
    start_offset INTEGER NOT NULL CHECK(start_offset>=0), end_offset INTEGER NOT NULL CHECK(end_offset>start_offset),
    speaker TEXT NOT NULL CHECK(speaker='Jesus'), evidence_source_id INTEGER NOT NULL REFERENCES bible_sources(id),
    evidence_locator TEXT NOT NULL CHECK(length(trim(evidence_locator))>0),
    UNIQUE(verse_id,start_offset,end_offset,evidence_source_id));
CREATE TABLE bible_annotations (
    id TEXT PRIMARY KEY, user_key TEXT NOT NULL CHECK(length(user_key)>0),
    verse_id INTEGER NOT NULL REFERENCES bible_verses(id), group_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('highlight','underline','note')),
    start_offset INTEGER NOT NULL CHECK(start_offset>=0), end_offset INTEGER NOT NULL CHECK(end_offset>=start_offset),
    selected_text TEXT NOT NULL, color TEXT, note TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1 CHECK(version>0), created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    CHECK(kind='note' OR end_offset>start_offset));
CREATE INDEX bible_annotation_owner ON bible_annotations(user_key,verse_id,id);
CREATE TABLE bible_bookmarks (
    id TEXT PRIMARY KEY, user_key TEXT NOT NULL CHECK(length(user_key)>0),
    verse_id INTEGER NOT NULL REFERENCES bible_verses(id), character_offset INTEGER NOT NULL DEFAULT 0 CHECK(character_offset>=0),
    label TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    UNIQUE(user_key,verse_id,character_offset));
CREATE INDEX bible_bookmark_owner ON bible_bookmarks(user_key,id);
CREATE TABLE bible_reader_state (
    user_key TEXT PRIMARY KEY CHECK(length(user_key)>0), edition_id INTEGER REFERENCES bible_editions(id),
    book_id INTEGER NOT NULL DEFAULT 1 REFERENCES bible_books(id), chapter INTEGER NOT NULL DEFAULT 1 CHECK(chapter BETWEEN 1 AND 150),
    verse INTEGER NOT NULL DEFAULT 1 CHECK(verse BETWEEN 1 AND 176), character_offset INTEGER NOT NULL DEFAULT 0 CHECK(character_offset>=0),
    progress_percent REAL NOT NULL DEFAULT 0 CHECK(progress_percent BETWEEN 0 AND 100),
    theme TEXT NOT NULL DEFAULT 'paper' CHECK(theme IN ('paper','light','dark')),
    font_size INTEGER NOT NULL DEFAULT 19 CHECK(font_size BETWEEN 14 AND 30),
    layout_mode TEXT NOT NULL DEFAULT 'split' CHECK(layout_mode IN ('split','bible','study')),
    split_ratio INTEGER NOT NULL DEFAULT 60 CHECK(split_ratio BETWEEN 20 AND 80),
    version INTEGER NOT NULL DEFAULT 1 CHECK(version>0), updated_at TEXT NOT NULL);
"""


DICTIONARY_SCHEMA_SQL = """
CREATE TABLE bible_dictionary_versions (
    id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL REFERENCES bible_sources(id),
    import_run_id INTEGER NOT NULL REFERENCES bible_import_runs(id),
    family TEXT NOT NULL CHECK(family IN ('mounce','vine')), is_active INTEGER NOT NULL DEFAULT 0 CHECK(is_active IN (0,1)),
    UNIQUE(source_id,family));
CREATE UNIQUE INDEX bible_active_dictionary ON bible_dictionary_versions(family) WHERE is_active=1;
CREATE TABLE bible_dictionary_entries (
    id INTEGER PRIMARY KEY, version_id INTEGER NOT NULL REFERENCES bible_dictionary_versions(id),
    locator TEXT NOT NULL, headword TEXT NOT NULL, section TEXT NOT NULL, language TEXT NOT NULL DEFAULT '',
    content_json TEXT NOT NULL, plain_text TEXT NOT NULL, preview TEXT NOT NULL,
    UNIQUE(version_id,locator));
CREATE TABLE bible_dictionary_terms (
    entry_id INTEGER NOT NULL REFERENCES bible_dictionary_entries(id), term TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('headword','transliteration','original','cross_reference')),
    display TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 1 CHECK(complete IN (0,1)),
    PRIMARY KEY(entry_id,term,kind));
CREATE INDEX bible_dictionary_term_lookup ON bible_dictionary_terms(term,kind,entry_id);
CREATE TABLE bible_dictionary_identifiers (
    entry_id INTEGER NOT NULL REFERENCES bible_dictionary_entries(id), scheme TEXT NOT NULL CHECK(scheme IN ('strong','gk')),
    language TEXT NOT NULL CHECK(language IN ('Hebrew','Greek')), number INTEGER NOT NULL CHECK(number>0),
    qualifier TEXT NOT NULL DEFAULT '', source_text TEXT NOT NULL,
    PRIMARY KEY(entry_id,scheme,language,number,qualifier));
CREATE INDEX bible_dictionary_number_lookup ON bible_dictionary_identifiers(scheme,language,number,entry_id);
CREATE TABLE bible_dictionary_references (
    entry_id INTEGER NOT NULL REFERENCES bible_dictionary_entries(id), book_id INTEGER NOT NULL REFERENCES bible_books(id),
    chapter INTEGER NOT NULL, verse INTEGER NOT NULL, end_chapter INTEGER NOT NULL, end_verse INTEGER NOT NULL,
    source_text TEXT NOT NULL,
    PRIMARY KEY(entry_id,book_id,chapter,verse,end_chapter,end_verse));
CREATE INDEX bible_dictionary_passage_lookup ON bible_dictionary_references(book_id,chapter,verse,entry_id);
CREATE TABLE bible_dictionary_assets (
    sha256 TEXT PRIMARY KEY CHECK(length(sha256)=64), mime_type TEXT NOT NULL CHECK(mime_type IN ('image/gif','image/jpeg','image/png')),
    data BLOB NOT NULL);
CREATE TABLE bible_dictionary_entry_assets (
    entry_id INTEGER NOT NULL REFERENCES bible_dictionary_entries(id), sha256 TEXT NOT NULL REFERENCES bible_dictionary_assets(sha256),
    PRIMARY KEY(entry_id,sha256));
CREATE TABLE bible_daily_insights (
    cache_key TEXT PRIMARY KEY, status TEXT NOT NULL CHECK(status IN ('pending','ready','no_match','failed')),
    lease_until REAL NOT NULL DEFAULT 0, result_json TEXT NOT NULL DEFAULT '{}', updated_at TEXT NOT NULL);
"""


ORIGINAL_SCHEMA_SQL = """
CREATE TABLE bible_original_versions (
    id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL UNIQUE REFERENCES bible_sources(id),
    import_run_id INTEGER NOT NULL REFERENCES bible_import_runs(id),
    is_active INTEGER NOT NULL DEFAULT 0 CHECK(is_active IN (0,1)),
    manifest_json TEXT NOT NULL, report_json TEXT NOT NULL DEFAULT '{}');
CREATE UNIQUE INDEX bible_active_original_version ON bible_original_versions(is_active) WHERE is_active=1;
CREATE TABLE bible_original_lexemes (
    id INTEGER PRIMARY KEY, version_id INTEGER NOT NULL REFERENCES bible_original_versions(id),
    extended_strong TEXT NOT NULL, standard_strong TEXT NOT NULL DEFAULT '',
    source_estrong TEXT NOT NULL, unified_strong TEXT NOT NULL DEFAULT '',
    language TEXT NOT NULL CHECK(language IN ('Hebrew','Aramaic','Greek')),
    lemma TEXT NOT NULL, transliteration TEXT NOT NULL DEFAULT '', morphology TEXT NOT NULL DEFAULT '',
    gloss TEXT NOT NULL DEFAULT '', definition TEXT NOT NULL DEFAULT '',
    source_file TEXT NOT NULL, source_line INTEGER NOT NULL CHECK(source_line>0), raw_json TEXT NOT NULL,
    UNIQUE(version_id,extended_strong));
CREATE INDEX bible_original_lemma ON bible_original_lexemes(lemma,version_id);
CREATE INDEX bible_original_strong ON bible_original_lexemes(standard_strong,version_id);
CREATE TABLE bible_original_tokens (
    id INTEGER PRIMARY KEY, version_id INTEGER NOT NULL REFERENCES bible_original_versions(id),
    book_id INTEGER NOT NULL REFERENCES bible_books(id), chapter INTEGER NOT NULL CHECK(chapter>0),
    verse INTEGER NOT NULL CHECK(verse>=0), token_position INTEGER NOT NULL CHECK(token_position>0),
    source_ref TEXT NOT NULL, language TEXT NOT NULL CHECK(language IN ('Hebrew','Aramaic','Greek')),
    spelling TEXT NOT NULL, token_kind TEXT NOT NULL CHECK(token_kind IN ('word','omission')),
    transliteration TEXT NOT NULL DEFAULT '',
    lemma TEXT NOT NULL DEFAULT '', morphology TEXT NOT NULL DEFAULT '', gloss TEXT NOT NULL DEFAULT '',
    text_type TEXT NOT NULL DEFAULT '', witnesses TEXT NOT NULL DEFAULT '',
    source_file TEXT NOT NULL, source_line INTEGER NOT NULL CHECK(source_line>0), raw_json TEXT NOT NULL,
    UNIQUE(version_id,source_file,source_line),
    CHECK((token_kind='word' AND length(spelling)>0) OR (token_kind='omission' AND spelling='')));
CREATE INDEX bible_original_passage ON bible_original_tokens(book_id,chapter,verse,version_id,token_position);
CREATE INDEX bible_original_token_lemma ON bible_original_tokens(lemma,version_id);
CREATE TABLE bible_original_token_identifiers (
    token_id INTEGER NOT NULL REFERENCES bible_original_tokens(id), ordinal INTEGER NOT NULL CHECK(ordinal>=0),
    extended_strong TEXT NOT NULL, standard_strong TEXT NOT NULL DEFAULT '',
    lexeme_id INTEGER REFERENCES bible_original_lexemes(id), role TEXT NOT NULL CHECK(role IN ('root','component')),
    PRIMARY KEY(token_id,ordinal));
CREATE INDEX bible_original_token_strong ON bible_original_token_identifiers(standard_strong,token_id);
CREATE INDEX bible_original_token_extended ON bible_original_token_identifiers(extended_strong,token_id);
CREATE INDEX bible_original_token_lexeme ON bible_original_token_identifiers(lexeme_id,token_id);
CREATE TABLE bible_original_dictionary_links (
    lexeme_id INTEGER NOT NULL REFERENCES bible_original_lexemes(id),
    entry_id INTEGER NOT NULL REFERENCES bible_dictionary_entries(id), standard_strong TEXT NOT NULL,
    match_method TEXT NOT NULL CHECK(match_method='explicit_standard_strong'),
    PRIMARY KEY(lexeme_id,entry_id));
CREATE TABLE bible_niv_alignments (
    id INTEGER PRIMARY KEY, verse_id INTEGER NOT NULL REFERENCES bible_verses(id),
    verse_content_hash TEXT NOT NULL CHECK(length(verse_content_hash)=64),
    tokenization_version TEXT NOT NULL DEFAULT 'niv-codepoint-v1' CHECK(tokenization_version='niv-codepoint-v1'),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','verified','rejected')),
    evidence_source_id INTEGER REFERENCES bible_sources(id), evidence_locator TEXT NOT NULL DEFAULT '',
    verification_method TEXT NOT NULL CHECK(verification_method IN ('source_word_alignment','curated_verified')),
    verified_by TEXT NOT NULL DEFAULT '', verified_at TEXT NOT NULL DEFAULT '',
    CHECK(status!='verified' OR (evidence_source_id IS NOT NULL AND length(trim(evidence_locator))>0
        AND length(trim(verified_by))>0 AND length(trim(verified_at))>0)));
CREATE INDEX bible_niv_alignment_verse ON bible_niv_alignments(verse_id,status,verse_content_hash);
CREATE TABLE bible_niv_alignment_tokens (
    alignment_id INTEGER NOT NULL REFERENCES bible_niv_alignments(id),
    token_id INTEGER NOT NULL REFERENCES bible_tokens(id), PRIMARY KEY(alignment_id,token_id));
CREATE INDEX bible_niv_alignment_token ON bible_niv_alignment_tokens(token_id,alignment_id);
CREATE TABLE bible_niv_alignment_originals (
    alignment_id INTEGER NOT NULL REFERENCES bible_niv_alignments(id),
    original_token_id INTEGER NOT NULL REFERENCES bible_original_tokens(id),
    PRIMARY KEY(alignment_id,original_token_id));
"""


THOMPSON_SCHEMA_SQL = """
CREATE TABLE bible_edition_details (
    edition_id INTEGER PRIMARY KEY REFERENCES bible_editions(id), label TEXT NOT NULL,
    source_kind TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE bible_chain_topics (
    id INTEGER PRIMARY KEY, edition_id INTEGER NOT NULL REFERENCES bible_editions(id),
    topic_key TEXT NOT NULL, title TEXT NOT NULL, content_json TEXT NOT NULL,
    source_page INTEGER NOT NULL, UNIQUE(edition_id,topic_key));
CREATE TABLE bible_chain_verses (
    verse_id INTEGER NOT NULL REFERENCES bible_verses(id),
    topic_id INTEGER NOT NULL REFERENCES bible_chain_topics(id), PRIMARY KEY(verse_id,topic_id));
CREATE INDEX bible_chain_topic_verses ON bible_chain_verses(topic_id,verse_id);
CREATE TABLE bible_dictionary_senses (
    id INTEGER PRIMARY KEY, entry_id INTEGER NOT NULL REFERENCES bible_dictionary_entries(id),
    language TEXT NOT NULL, block_start INTEGER NOT NULL, block_end INTEGER NOT NULL,
    UNIQUE(entry_id,block_start));
CREATE TABLE bible_dictionary_sense_identifiers (
    sense_id INTEGER NOT NULL REFERENCES bible_dictionary_senses(id),
    scheme TEXT NOT NULL, language TEXT NOT NULL, number INTEGER NOT NULL,
    qualifier TEXT NOT NULL DEFAULT '', source_text TEXT NOT NULL,
    PRIMARY KEY(sense_id,scheme,language,number,qualifier));
CREATE INDEX bible_sense_identifier_lookup ON bible_dictionary_sense_identifiers(scheme,language,number,sense_id);
CREATE INDEX bible_sense_entry ON bible_dictionary_senses(entry_id,language);
CREATE TABLE bible_navigation_history (
    id INTEGER PRIMARY KEY, user_key TEXT NOT NULL CHECK(length(user_key)>0),
    verse_id INTEGER NOT NULL REFERENCES bible_verses(id), visited_at TEXT NOT NULL);
CREATE INDEX bible_history_owner ON bible_navigation_history(user_key,id DESC);
"""


CONCORDANCE_SCHEMA_SQL = """
CREATE TABLE bible_thompson_concordance (
    id INTEGER PRIMARY KEY, edition_id INTEGER NOT NULL REFERENCES bible_editions(id),
    headword TEXT NOT NULL, normalized_term TEXT NOT NULL, related_json TEXT NOT NULL,
    content_json TEXT NOT NULL, source_page INTEGER NOT NULL,
    UNIQUE(edition_id,normalized_term));
CREATE INDEX bible_thompson_concordance_term ON bible_thompson_concordance(normalized_term,edition_id);
"""


STUDY_SCHEMA_SQL = """
CREATE TABLE bible_thompson_studies (
    id INTEGER PRIMARY KEY, edition_id INTEGER NOT NULL REFERENCES bible_editions(id),
    study_key TEXT NOT NULL, kind TEXT NOT NULL, title TEXT NOT NULL, book_id INTEGER REFERENCES bible_books(id),
    aliases_json TEXT NOT NULL, content_json TEXT NOT NULL, source_page INTEGER NOT NULL,
    UNIQUE(edition_id,study_key));
CREATE TABLE bible_thompson_study_pages (
    id INTEGER PRIMARY KEY, edition_id INTEGER NOT NULL REFERENCES bible_editions(id),
    source_page INTEGER NOT NULL, image_png BLOB NOT NULL, UNIQUE(edition_id,source_page));
CREATE TABLE bible_person_profiles (
    id INTEGER PRIMARY KEY, edition_id INTEGER NOT NULL REFERENCES bible_editions(id),
    person_key TEXT NOT NULL, name TEXT NOT NULL, label TEXT NOT NULL,
    aliases_json TEXT NOT NULL, content_json TEXT NOT NULL, citations_json TEXT NOT NULL,
    UNIQUE(edition_id,person_key));
"""


class InvalidBibleSelection(Exception):
    """User input errors must not be mistaken for database path failures."""


class StorageUnavailable(Exception):
    pass


class SyncError(Exception):
    """Safe operator-facing sync error; never include credentials or ebook HTML."""


_PDFIUM_LOCK = threading.Lock()


def _pdf_clean(value):
    value = re.sub(r'[\ufffe\u00ad\x02]\s*', '', value)
    return _text(value.replace('\u200b', '').replace('\u200c', '').replace('\ufeff', ''))


def _thompson_glyphs(page):
    """Source glyph properties distinguish scripture, headings and margin notes.

    Fonts are inspected, never extracted or embedded as web fonts. All PDFium
    access stays under the importer's process-local mutex (PDFium is not thread-safe).
    """
    import pypdfium2 as pdfium
    text = page.get_textpage()
    font_buffer, flags = ctypes.create_string_buffer(160), ctypes.c_int()
    matrix = pdfium.raw.FS_MATRIX()
    channels = [ctypes.c_uint() for _ in range(4)]
    try:
        for index in range(text.count_chars()):
            code = pdfium.raw.FPDFText_GetUnicode(text, index)
            char = chr(code) if 0 < code <= 0x10ffff else ''
            if not char:
                continue
            if char.isspace():
                yield dict(char=char, font='', size=0, x=0, y=0, red=False, index=index)
                continue
            pdfium.raw.FPDFText_GetFontInfo(text, index, font_buffer, len(font_buffer), ctypes.byref(flags))
            font = font_buffer.value.decode('utf-8', 'replace')
            pdfium.raw.FPDFText_GetMatrix(text, index, ctypes.byref(matrix))
            box = text.get_charbox(index)
            pdfium.raw.FPDFText_GetFillColor(text, index, *[ctypes.byref(v) for v in channels])
            red, green, blue, alpha = (v.value for v in channels)
            yield dict(char=char, font=font, size=abs(matrix.d), x=box[0], y=matrix.f,
                       red=alpha>0 and red>100 and red>green*1.7 and red>blue*1.4, index=index)
    finally:
        text.close()


def _thompson_verse_text(characters):
    """Normalize only PDF line wrapping; keep a color mask through normalization."""
    result, colors, joining = [], [], False
    for char, red in characters:
        if char in '\ufffe\u00ad\x02':
            joining = True
            continue
        if char in '\u200b\u200c\ufeff':
            continue
        if char.isspace():
            if not joining and result and result[-1]!=' ':
                result.append(' '); colors.append(red)
            continue
        joining = False
        result.append(char); colors.append(red)
    if result and result[-1]==' ':
        result.pop(); colors.pop()
    spans, start = [], None
    for index, red in enumerate(colors+[False]):
        if red and start is None:
            start=index
        elif not red and start is not None:
            spans.append((start,index)); start=None
    return ''.join(result), spans


def _thompson_topic_blocks(lines):
    """Keep source paragraph boundaries and emphasis, while unwrapping PDF lines."""
    groups = []
    for line in lines:
        visible = [g for g in line if not g['char'].isspace()]
        if not visible:
            continue
        first = visible[0]
        label = _pdf_clean(''.join(g['char'] for g in line))
        font = first['font']
        bold = 'Semibold' in font or 'Bold' in font
        if first['char'] == '\uf0aa':
            role = 'related'
        elif first['red'] and bold and SCRIPTURE_PATTERN.match(label):
            role = 'verse'
        elif 'Italic' in font and bold:
            role = 'example' if scripture_references(label) else 'subheading'
        elif bold and 'SansClosed' in font:
            role = 'intro'
        elif (bold and not first['red']) or re.match(r'^(?:\d+|[a-z])\.\s+[A-Z]', label):
            role = 'section'
        else:
            role = None  # A wrapped line continues the preceding source paragraph.
        continued_heading = (groups and role in ('section', 'subheading')
            and groups[-1][0] == role and not re.match(r'^(?:\d+|[a-z])\.\s', label))
        if not groups or (role is not None and not continued_heading):
            groups.append((role or 'paragraph', []))
        groups[-1][1].extend(line)
        groups[-1][1].append(dict(char=' ', font='', red=False))

    blocks = []
    for role, glyphs in groups:
        # Normalize with the same soft-wrap rules as scripture, retaining each
        # character's source font so bold and italic labels survive extraction.
        chars, marks, joining = [], [], False
        for glyph in glyphs:
            char = glyph['char']
            if char in '\ufffe\u00ad\x02':
                joining = True
                continue
            if char in '\u200b\u200c\ufeff':
                continue
            if char.isspace():
                if chars and chars[-1] != ' ' and not joining:
                    chars.append(' '); marks.append([])
                continue
            joining = False
            style = []
            if 'Italic' in glyph['font']:
                style.append('em')
            if 'Semibold' in glyph['font'] or 'Bold' in glyph['font']:
                style.append('strong')
            chars.append('\u2192' if char == '\uf0aa' else char); marks.append(style)
        if chars and chars[-1] == ' ':
            chars.pop(); marks.pop()
        text = ''.join(chars)
        refs = scripture_references(text)
        # The printed index puts section labels and compact reference lists on
        # the same line. Give each a block, preserving inherited citation context.
        split = refs[0]['start'] if refs and role in ('intro', 'section') else len(text)
        ranges = [(0, split, role)]
        if split < len(text):
            ranges.append((split, len(text), 'references'))
        for start, end, block_role in ranges:
            while start < end and text[start].isspace(): start += 1
            while end > start and text[end-1].isspace(): end -= 1
            if start == end:
                continue
            runs = []
            for char, style in zip(chars[start:end], marks[start:end]):
                if runs and runs[-1]['marks'] == style:
                    runs[-1]['text'] += char
                else:
                    runs.append(dict(type='text', text=char, marks=style))
            references = [dict(ref, start=ref['start']-start, end=ref['end']-start)
                          for ref in refs if start <= ref['start'] < ref['end'] <= end]
            blocks.append(dict(type='heading' if block_role in ('section', 'subheading') else 'paragraph',
                role=block_role, text=text[start:end], runs=runs, references=references))
    return blocks


def _parse_thompson_index(pdf, progress=None):
    """Read the inspected index's columns in source order, excluding page furniture."""
    topics, current, topic_lines = {}, None, []

    def finish():
        if current is None:
            return
        key, source_page = current
        blocks = _thompson_topic_blocks(topic_lines)
        if not blocks or key in topics:
            raise SyncError('The Thompson index has an empty or duplicate topic.')
        plain = ' '.join(block['text'] for block in blocks)
        title = blocks[0]['text'].split(',')[0].strip()
        topics[key] = dict(title=title, text=plain, blocks=blocks,
                          source_page=source_page, references=scripture_references(plain))

    for page_index in range(1537, 1887):
        if progress and (page_index-1537) % 25 == 0:
            progress('Arranging Thompson chain index: PDF page ' + str(page_index+1) + '.')
        page, lines, line, baseline = pdf[page_index], [], [], None
        try:
            for glyph in _thompson_glyphs(page):
                if glyph['char'].isspace():
                    if line: line.append(glyph)
                    continue
                # Body text is 6.5/7.5pt. Larger group titles and the 8.8pt
                # printed page number do not belong to the preceding topic.
                if not (80 < glyph['y'] < 680 and glyph['size'] < 8.3
                        and ('ZNIV' in glyph['font'] or 'Wingdings' in glyph['font'])):
                    continue
                if baseline is not None and abs(glyph['y']-baseline) > 2:
                    lines.append(line); line = []
                baseline = glyph['y']
                line.append(glyph)
            if line: lines.append(line)
        finally:
            page.close()
        for line in lines:
            raw = ''.join(g['char'] for g in line)
            marker = re.match(r'\s*\uf074\s*(\d{1,4}[a-z]?)\s+', raw)
            if marker:
                finish()
                current, topic_lines = (marker[1], page_index+1), []
                line = line[marker.end():]
            if current is not None:
                topic_lines.append(line)
    finish()
    if len(topics) < 4000 or not {'884', '2201', '2729'}.issubset(topics):
        raise SyncError('Thompson chain-topic validation failed; the existing index was kept.')
    return topics


def format_thompson_index(database_path, pdf_path, *, backup_directory=None, dry_run=False):
    """Upgrade only topic presentation; retain topic IDs, verse links and reader data."""
    import pypdfium2 as pdfium
    path = _database_path(database_path)
    data = Path(pdf_path).read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    lock = _sync_lock(path, create=True)
    if lock is None:
        raise SyncError('A Bible Study sync is already running.')
    try:
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as conn:
            editions = conn.execute('''SELECT e.id FROM bible_editions e
                JOIN bible_sources s ON s.id=e.source_id JOIN bible_edition_details d ON d.edition_id=e.id
                WHERE e.is_active=1 AND d.source_kind='thompson_pdf' AND s.sha256=?''', (digest,)).fetchall()
            if len(editions) != 1:
                raise SyncError('The PDF must match the currently indexed Thompson source exactly.')
            edition_id = editions[0][0]
            stored = dict(conn.execute('SELECT topic_key,id FROM bible_chain_topics WHERE edition_id=?', (edition_id,)))
        with _PDFIUM_LOCK, pdfium.PdfDocument(data) as pdf:
            topics = _parse_thompson_index(pdf)
        if topics.keys() != stored.keys():
            raise SyncError('Topic keys differ from the stored index; existing topics were kept.')
        report = dict(topics=len(topics), blocks=sum(len(t['blocks']) for t in topics.values()), dry_run=dry_run)
        if dry_run:
            return report
        directory = Path(backup_directory) if backup_directory else path.parent/'backups'
        directory.mkdir(parents=True, exist_ok=True)
        backup = directory/('bible_study_before_chain_layout_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_')+secrets.token_hex(3)+'.db')
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as source, closing(sqlite3.connect(backup)) as target:
            source.backup(target)
            if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise SyncError('Backup validation failed; the existing index was kept.')
        with closing(sqlite3.connect(path.as_uri()+'?mode=rw', uri=True)) as conn, conn:
            conn.execute('BEGIN IMMEDIATE')
            conn.executemany('UPDATE bible_chain_topics SET content_json=? WHERE id=? AND edition_id=?',
                [(json.dumps(topic['blocks'], ensure_ascii=False), stored[key], edition_id) for key, topic in topics.items()])
        return dict(report, backup=str(backup))
    finally:
        _release_sync_lock(lock)


def _thompson_concordance_entries(lines):
    """Resolve the concordance's explicitly inherited book/chapter prefixes."""
    entries, entry, context = [], None, None
    prefix = re.compile(r'^(?:(?P<book>'+_BOOK_PATTERN+r')\s*)?'
                        r'(?P<chapter>\d{1,3})?\s*:\s*(?P<verse>\d{1,3})(?=\s+\S)', re.I)
    for source_page, heading, raw in lines:
        text = _pdf_clean(raw)
        if entry and entry['heading'].count('(') > entry['heading'].count(')'):
            entry['heading'] += ' '+text
            continue
        if heading:
            entry = dict(heading=text, source_page=source_page, blocks=[])
            entries.append(entry)
            continue
        if entry is None:
            continue
        match = prefix.match(text)
        if match:
            book = (_REF_NAMES.get(re.sub(r'[.\s]+','',match['book']).casefold())
                    if match['book'] else context[0] if context else None)
            chapter = int(match['chapter']) if match['chapter'] else (1 if book and BOOKS[book-1][1]==1 else context[1] if context else None)
            if not book or not chapter:
                raise SyncError('A Thompson concordance reference has no source book/chapter context: '+entry['heading']+'; PDF page '+str(source_page)+'; '+text)
            _, chapter, verse = _reference(book, chapter, int(match['verse']))
            context = book, chapter
            reference = BOOKS[book-1][0]+' '+str(chapter)+':'+str(verse)
            entry['blocks'].append(dict(type='paragraph', role='excerpt', raw=raw,
                text=reference+' '+text[match.end():].lstrip(), references=scripture_references(reference)))
        elif entry['blocks']:
            previous = entry['blocks'][-1]
            previous['text'] = _pdf_clean(previous['text']+' '+raw)
            previous['raw'] += '\n'+raw
            if previous.get('role') != 'excerpt':
                previous['references'] = scripture_references(re.sub(r':\s+', ':', previous['text']))
        else:
            text = re.sub(r':\s+', ':', text)
            entry['blocks'].append(dict(type='paragraph', text=text, raw=raw, references=scripture_references(text)))
    for entry in entries:
        see = re.split(r'\s+See\s+',entry['heading'],maxsplit=1)
        entry['headword'] = see[0].split(' (',1)[0].strip()
        if len(see)>1:
            entry['related'] = [term.strip(' .') for term in see[1].split(',')]
        else:
            forms = entry['heading'].split(' (',1)[1].split(';',1)[0] if ' (' in entry['heading'] else ''
            entry['related'] = re.findall(r"[A-Z]+(?:['\u2019-][A-Z]+)*\*?",forms)
        entry['normalized_term'] = normalize_term(entry['headword'])
        for block in entry['blocks']:
            text = _pdf_clean(block['raw'])
            if block.get('role')=='excerpt':
                match=prefix.match(text)
                block['text']=block['references'][0]['source_text']+' '+text[match.end():].lstrip()
                continue
            block['text'] = re.sub(r':\s+', ':',text)
            block['references'] = [ref for ref in scripture_references(block['text'])
                if ':' in ref['source_text'] or not re.search(r';\s*$',block['text'][:ref['start']])]
    return entries


def _parse_thompson_concordance(pdf, progress=None):
    lines = []
    for page_index in range(1967, 2025):
        if progress and (page_index-1967) % 15 == 0:
            progress('Indexing Thompson concordance: PDF page '+str(page_index+1)+'.')
        page, glyph_lines, line, baseline = pdf[page_index], [], [], None
        try:
            for glyph in _thompson_glyphs(page):
                if glyph['char'].isspace():
                    if line: line.append(glyph)
                    continue
                if not (80 < glyph['y'] < 680 and 4 < glyph['size'] < 6.4 and 'ZNIVSans' in glyph['font']):
                    continue
                if baseline is not None and abs(glyph['y']-baseline)>2:
                    glyph_lines.append(line); line=[]
                baseline=glyph['y']; line.append(glyph)
            if line: glyph_lines.append(line)
        finally:
            page.close()
        for line in glyph_lines:
            visible=[g for g in line if not g['char'].isspace()]
            heading=bool(visible) and 'Semibold' in visible[0]['font']
            lines.append((page_index+1, heading, ''.join(g['char'] for g in line)))
    entries = _thompson_concordance_entries(lines)
    terms = [e['normalized_term'] for e in entries]
    if not 2400<=len(entries)<=2550 or len(set(terms))!=len(terms) or not {'obey','love','faith','aaron','zion'}.issubset(terms):
        raise SyncError('Thompson concordance validation failed; existing entries were kept.')
    return entries


def _publish_thompson_concordance(conn, edition_id, entries):
    for entry in entries:
        conn.execute('''INSERT INTO bible_thompson_concordance
            (edition_id,headword,normalized_term,related_json,content_json,source_page) VALUES(?,?,?,?,?,?)
            ON CONFLICT(edition_id,normalized_term) DO UPDATE SET related_json=excluded.related_json,
            content_json=excluded.content_json,source_page=excluded.source_page''',
            (edition_id,entry['headword'],entry['normalized_term'],json.dumps(entry['related']),
             json.dumps(entry['blocks'],ensure_ascii=False),entry['source_page']))


def _study_page_lines(page):
    lines, line, baseline = [], [], None
    for glyph in _thompson_glyphs(page):
        if glyph['char'].isspace():
            if line: line.append(glyph)
            continue
        if not 80 < glyph['y'] < 680 or not ('ZNIV' in glyph['font'] or 'Wingdings' in glyph['font']):
            continue
        if baseline is not None and abs(glyph['y']-baseline)>2:
            lines.append(line); line=[]
        baseline=glyph['y']; line.append(glyph)
    if line: lines.append(line)
    return lines


def _study_references(text, book_id=None):
    refs=scripture_references(text)
    # Bare chapter:verse citations in a book introduction refer to that book.
    if book_id:
        for match in re.finditer(_RANGE_PATTERN, text):
            if any(match.start()<ref['end'] and match.end()>ref['start'] for ref in refs):continue
            parsed=scripture_references(BOOKS[book_id-1][0]+' '+match[0])
            if parsed:refs.append(dict(parsed[0],start=match.start(),end=match.end(),source_text=match[0]))
    # A semicolon followed by a lone number in an article can denote a chapter.
    return sorted([r for r in refs if ':' in r['source_text'] or not re.search(r';\s*$',text[:r['start']])],key=lambda r:r['start'])


def _study_blocks(lines, book_id=None):
    groups=[]
    for line in lines:
        visible=[g for g in line if not g['char'].isspace()]
        if not visible:continue
        text=_pdf_clean(''.join(g['char'] for g in line))
        first=visible[0]; bold='Semibold' in first['font']
        all_bold=all('Semibold' in g['font'] for g in visible)
        numbered=bool(re.match(r'^(?:[IVX]+|[A-Z]|\d+)\.\s',text))
        new=not groups or bold or numbered or groups[-1]['type']=='heading'
        if new:groups.append(dict(type='heading' if all_bold else 'paragraph',glyphs=[]))
        groups[-1]['glyphs'].extend(line)
        groups[-1]['glyphs'].append(dict(char=' ',font=''))
    blocks=[]
    for group in groups:
        # Keep the font mask while removing source soft hyphens and line wraps.
        chars=[];styles=[];joining=False
        for glyph in group['glyphs']:
            char=glyph['char']
            if char in '\ufffe\u00ad\x02':joining=True;continue
            if char in '\u200b\u200c\ufeff':continue
            if char.isspace():
                if chars and chars[-1]!=' ' and not joining:chars.append(' ');styles.append([])
                continue
            joining=False
            chars.append('\u2192' if char in '\uf0a8\uf0aa' else char)
            styles.append((['strong'] if 'Semibold' in glyph['font'] else [])+(['em'] if 'Italic' in glyph['font'] else []))
        if chars and chars[-1]==' ':chars.pop();styles.pop()
        text=''.join(chars);runs=[]
        if not text:continue
        for char,marks in zip(chars,styles):
            if runs and runs[-1]['marks']==marks:runs[-1]['text']+=char
            else:runs.append(dict(type='text',text=char,marks=marks))
        blocks.append(dict(type=group['type'],text=text,runs=runs,references=_study_references(text,book_id)))
    return blocks


def _parse_condensed_outlines(pdf):
    """Keep each printed entry (including combined books) in its own outline."""
    entries, current, headings = {}, None, []
    for number in range(1888, 1891):
        page = pdf[number-1]
        try:
            for line in _study_page_lines(page):
                first = next(g for g in line if not g['char'].isspace())
                if not 8.5 < first['size'] < 11.5:
                    continue
                text = _pdf_clean(''.join(g['char'] for g in line))
                if first['size'] >= 10:
                    if text in ('OLD TESTAMENT', 'NEW TESTAMENT'):
                        headings = [text]
                    elif text.startswith('(') and len(headings) > 1:
                        headings[-1] += ' ' + text
                    else:
                        headings = headings[:1] + [text]
                    current = None
                    continue
                match = re.match(r'^[\d, ]+\.\s+((?:42\d\d)(?:,\s*42\d\d)*)\s+', text)
                if match:
                    current = dict(keys=re.findall(r'42\d\d', match[1]), lines=[], headings=list(headings), source_page=number)
                    for key in current['keys']:
                        entries[int(key)-4219] = current
                if current:
                    current['lines'].append(line)
        finally:
            page.close()
    if set(entries) != set(range(1, 67)):
        raise SyncError('The condensed outline does not cover all 66 books.')
    return {book: dict(source_page=entry['source_page'], blocks=[dict(type='heading', text=h, references=[])
                for h in entry['headings']] + _study_blocks(entry['lines'], book)) for book, entry in entries.items()}


def _book_chain_blocks(conn, edition_id, blocks):
    """Only interpret dotted leaders inside a printed Major Chains section."""
    targets = {r['topic_key']: r['id'] for r in conn.execute(
        'SELECT id,topic_key FROM bible_chain_topics WHERE edition_id=?', (edition_id,))}
    result, in_chains = [], False
    for block in blocks:
        if block['type'] == 'heading':
            in_chains = bool(re.match(r'MAJOR CHAINS\b', block['text'], re.I))
        if not in_chains or block['type'] == 'heading':
            result.append(block)
            continue
        pattern = r'(.+?)(?:\.\s*){2,}(\d{1,4}[a-z]?(?:,\s*\d{1,4}[a-z]?)*)(?!\d)'
        matches = list(re.finditer(pattern, block['text']))
        if not matches:
            result.append(block)
            continue
        for match in matches:
            label, keys = match[1].strip(' .'), re.findall(r'\d{1,4}[a-z]?',match[2])
            # One source row prints its first key before the dotted leaders.
            embedded = re.search(r'\s+(\d{3,4}[a-z]?)$', label)
            if embedded:
                keys.insert(0,embedded[1]);label=label[:embedded.start()].rstrip()
            for key in keys:
                text = label + ' · ' + key
                result.append(dict(type='paragraph', role='major_chain', text=text, references=[],
                    chain_links=[dict(start=0, end=len(text), key=key, title=label, topic_id=targets.get(key))]))
    return result


def _parse_thompson_studies(pdf, starts=None, progress=None):
    """Index the inspected book introductions, character studies and source charts."""
    if starts is None:
        page=pdf[5];text=page.get_textpage()
        try:contents=text.get_text_range()
        finally:text.close();page.close()
        starts=[]
        for line in contents.splitlines():
            match=re.match(r'^\s*([1-3]?\s*[A-Za-z ]+),.*?(\d+)\s*$',line)
            book=BOOK_BY_NAME.get(_text(match[1]).casefold()) if match else None
            if book:starts.append((book[0],int(match[2])+20))
    if [book for book,_ in starts]!=list(range(1,67)):
        raise SyncError('Thompson book-introduction boundaries could not be verified.')
    studies=[];classifications=[];images={}
    for ordinal,(book,start) in enumerate(starts):
        if progress and book%10==1:progress('Indexing Thompson book studies: '+BOOKS[book-1][0]+'.')
        lines=[]
        end=starts[ordinal+1][1] if ordinal+1<len(starts) else start+3
        for page_index in range(start,min(start+3,end)):
            page=pdf[page_index]
            try:
                filtered=[];page_lines=_study_page_lines(page)
                scripture_start=min((g['index'] for line in page_lines for g in line if 'SerifClosed11-Regular' in g['font']
                    and 8.3<g['size']<10.5),default=None)
                for line in page_lines:
                    vis=[g for g in line if not g['char'].isspace()]
                    if vis and all(6.8<g['size']<7.2 for g in vis) and (scripture_start is None or vis[0]['index']<scripture_start):filtered.append(line)
                if not filtered:break
                lines.extend(filtered)
                if scripture_start is not None:break
            finally:page.close()
        blocks=_study_blocks(lines,book)
        if not blocks:raise SyncError('A Thompson book introduction is missing: '+BOOKS[book-1][0]+'.')
        studies.append(dict(key='book:'+str(book),kind='book',title=BOOKS[book-1][0],book_id=book,
                            aliases=[],blocks=blocks,source_page=start+1))
    current=None
    for page_index in range(1892,1902):
        page=pdf[page_index]
        try:
            for line in _study_page_lines(page):
                vis=[g for g in line if not g['char'].isspace()]
                if not vis:continue
                first=vis[0];label=_pdf_clean(''.join(g['char'] for g in line))
                marker=re.match(r'^(428\d|429[0-7]|430[01])\s+(.+)',label)
                if marker and first['size']>10:
                    if current:
                        current['blocks']=_study_blocks(current.pop('lines'));studies.append(current)
                    name=marker[2].split(',')[0].strip()
                    current=dict(key='character:'+marker[1],kind='character',title=marker[2],book_id=None,
                        aliases=[name],source_page=page_index+1,lines=[])
                elif current and all('SerifClosed' in g['font'] and 8.5<g['size']<10 for g in vis):
                    current['lines'].append(line)
        finally:page.close()
    if current:current['blocks']=_study_blocks(current.pop('lines'));studies.append(current)
    category='';item=None
    for page_index in range(1902,1905):
        page=pdf[page_index]
        try:lines=_study_page_lines(page)
        finally:page.close()
        body=[line for line in lines if abs(next(g for g in line if not g['char'].isspace())['size']-7.6)<.15]
        minimum={column:min((next(g for g in line if not g['char'].isspace())['x'] for line in body
                 if int(next(g for g in line if not g['char'].isspace())['x']//126)==column),default=0)
                 for column in range(4)}
        for line in lines:
            first=next(g for g in line if not g['char'].isspace());raw=''.join(g['char'] for g in line);label=_pdf_clean(raw)
            if 8<first['size']<11 and 'Semibold' in first['font']:
                if item:classifications.append(item);item=None
                category=label;continue
            if abs(first['size']-7.6)<.15 and first['x']<minimum[int(first['x']//126)]+2:
                match=re.match(r'^([A-Z][A-Za-z\u2019\u2018 .()\'-]+?)(?:,|\s+\u201c|\s+\()',label)
                if match and not label.startswith(('Note:','For ')):
                    if item:classifications.append(item)
                    item=dict(name=match[1].strip(),text=raw,category=category,source_page=page_index+1)
                    continue
            if item and first['size']<8:item['text']+=raw
    if item:classifications.append(item)
    for item in classifications:
        item['text']=_pdf_clean(item['text'])
        item['text']=re.sub(r'(?:\.\s*){3,}',' — ',item['text'])
    outlines = _parse_condensed_outlines(pdf)
    # Charts retain their layout as authenticated, pre-rendered source pages.
    for study in studies:
        if study['kind']!='book':continue
        study['outline'] = outlines[study['book_id']]
        study['pages']=[]
        for number in range(1888,1891):
            page=pdf[number-1];text=page.get_textpage()
            try:source_text=text.get_text_range()
            finally:text.close();page.close()
            if re.search(r'\b'+str(4219+study['book_id'])+r'\b',source_text):study['pages'].append(number)
    chart_groups=[('condensed','Condensed Outline of the Bible',1888,1890),
        ('Abraham','Journeys of Abraham',1913,1914),('Moses','Overview of Moses’ Life',1915,1916),
        ('Joshua','Journeys of Joshua',1919,1920),('Gideon','Journeys of Gideon',1921,1921),
        ('Samuel','Journeys of Samuel',1922,1922),('David','Journeys of David',1923,1924),
        ('Solomon','Journeys of Solomon',1925,1926),('Jesus','Overview of Jesus’ Life',1927,1944),
        ('Paul','Overview and Journeys of Paul',1945,1954)]
    for name,title,start,end in chart_groups:
        if progress:progress('Preparing Thompson source charts: '+name+'.')
        for number in range(start,end+1):
            page=pdf[number-1]
            try:
                bitmap=page.render(scale=1.4)
                try:
                    buffer=io.BytesIO();bitmap.to_pil().save(buffer,format='PNG');images[number]=buffer.getvalue()
                finally:bitmap.close()
            finally:page.close()
        studies.append(dict(key='charts:'+name.casefold(),kind='chart',title=title,book_id=None,
                            aliases=[] if name=='condensed' else [name],source_page=start,blocks=[],pages=list(range(start,end+1))))
    if len([s for s in studies if s['kind']=='book'])!=66 or len(classifications)<120:
        raise SyncError('Thompson study-material coverage validation failed.')
    return dict(studies=studies,classifications=classifications,images=images)


def _publish_thompson_studies(conn, edition_id, material):
    profiles=[];by_name={}
    known={normalize_term(i['name'].split(',')[0]) for i in material['classifications']}
    known.update(normalize_term(a) for s in material['studies'] for a in s['aliases'])
    def register(profile):
        profiles.append(profile)
        for alias in profile['aliases']:by_name.setdefault(alias,[]).append(profile)
    for row in conn.execute('SELECT headword,normalized_term,content_json,source_page FROM bible_thompson_concordance WHERE edition_id=?',(edition_id,)):
        name,term,content,page=row
        articles=[b for b in json.loads(content) if b.get('role')!='excerpt']
        text=' '.join(b['text'] for b in articles)
        if not text:continue
        if term not in known and not re.search(r'\b(?:son|daughter|king|queen|priest|prophet|apostle|disciple|wife|mother|husband|brother|sister|emperor|Levite)\b',text[:140],re.I):continue
        if re.match(r'^(?:A |An |The )?(?:city|town|region|land|mountain|river|valley|village|place|country|district|island|province|territory|capital)\b',text,re.I):continue
        parts=re.split(r'(?<!\S)(?=\d+\.\s+[A-Z])',text)
        nonperson=re.compile(r'^(?:(?:A|An|The)\s+)?(?:tribe|region|kingdom|land|city|territory|nation|country|district|name used for people)\b',re.I)
        context_required=any(b.get('role')=='excerpt' for b in json.loads(content)) or any(nonperson.match(re.sub(r'^\d+\.\s*','',p.strip())) for p in parts)
        for ordinal,part in enumerate(p for p in parts if p.strip()):
            part=re.sub(r'^\d+\.\s*','',part.strip())
            if re.match(r'^See\b',part) or nonperson.match(part):continue
            blocks=[dict(type='paragraph',text=part,references=_study_references(part))]
            identity=re.split(r'\s*\(',part,maxsplit=1)[0].split('. ')[0].strip().rstrip('.')
            label=name.title()+' — '+identity[:130]
            aliases=[term]
            for match in re.finditer(r'\b(?:also called|also known as|called)\s+([A-Z][a-z]+)\b',part[:180]):
                aliases.append(normalize_term(match[1]))
            profile=dict(key='bio:'+term+':'+str(ordinal),name=name.title(),label=label,aliases=list(dict.fromkeys(aliases)),
                sections=[dict(title='Thompson concordance biography',source_page=page,blocks=blocks)],citations=blocks[0]['references'],topic_keys=[],context_required=context_required)
            register(profile)
    targets={}
    for key,content in conn.execute('SELECT topic_key,content_json FROM bible_chain_topics WHERE edition_id=?',(edition_id,)):
        targets[key]=[ref for block in json.loads(content) for ref in block.get('references',[])]
    def candidates(name):
        exact=by_name.get(normalize_term(name),[])
        if exact:return exact
        return by_name.get(normalize_term(name.split(' (')[0].split(' the ')[0]),[]) or by_name.get(normalize_term(name.split()[0]),[])
    def choose(options,refs):
        if len(options)==1:return options[0]
        scores=[]
        for profile in options:
            score=sum(any(a['book_id']==b['book_id'] and (b['chapter'],b['verse'])<=(a['chapter'],a['verse'])<=(b['end_chapter'],b['end_verse'])
                          for b in profile['citations']) for a in refs)
            scores.append((score,profile))
        scores.sort(key=lambda item:item[0],reverse=True)
        return scores[0][1] if scores and scores[0][0]>0 and (len(scores)==1 or scores[0][0]>scores[1][0]) else None
    for index,item in enumerate(material['classifications']):
        text=item['text'];keys=[]
        suffix=text.split(' — ',1)[-1] if ' — ' in text else ''
        if suffix:
            keys=[m[0] for m in re.finditer(r'\b\d{1,4}[a-z]?\b',suffix) if m[0] in targets]
        refs=_study_references(text)+[r for key in keys for r in targets[key]]
        options=candidates(item['name'])
        # A shared verse can mention several Marys. Explicit family identities
        # must not be merged merely because their citations overlap.
        relationship=re.search(r'\b(?:mother|father|husband|wife) of [A-Z][A-Za-z]+(?: [A-Z][A-Za-z]+)?',text)
        if relationship and len(options)>1:
            options=[p for p in options if relationship[0].casefold() in p['label'].casefold()]
        if 'Magdalene' in item['name']:options=[p for p in options if 'magdalene' in p['label'].casefold()]
        if 'Baptist' in item['name']:options=[p for p in options if 'baptist' in ' '.join(b['text'] for s in p['sections'] for b in s['blocks'])[:400].casefold()]
        profile=choose(options,refs)
        if profile is None:
            aliases=[normalize_term(item['name'])]
            # A qualified printed name is offered under its first name as an
            # explicit choice, never silently assigned to another identity.
            if len(item['name'].split())>1:aliases.append(normalize_term(item['name'].split()[0]))
            profile=dict(key='classified:'+str(index),name=item['name'],label=text.split(' — ',1)[0][:160],
                aliases=aliases,sections=[],citations=refs,topic_keys=[]);register(profile)
        if profile:
            alias=normalize_term(item['name'])
            if alias not in profile['aliases']:
                profile['aliases'].append(alias);by_name.setdefault(alias,[]).append(profile)
            profile['sections'].append(dict(title='Prominent Characters Classified — '+item['category'],source_page=item['source_page'],
                blocks=[dict(type='paragraph',text=text,references=_study_references(text))]))
            profile['topic_keys'].extend(keys)
    for study in material['studies']:
        conn.execute('''INSERT INTO bible_thompson_studies(edition_id,study_key,kind,title,book_id,aliases_json,content_json,source_page)
            VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(edition_id,study_key) DO UPDATE SET title=excluded.title,
            content_json=excluded.content_json,aliases_json=excluded.aliases_json,source_page=excluded.source_page''',
            (edition_id,study['key'],study['kind'],study['title'],study['book_id'],json.dumps(study['aliases']),
             json.dumps(dict(blocks=study['blocks'],pages=study.get('pages',[]),outline=study.get('outline')),ensure_ascii=False),study['source_page']))
        if study['kind']=='book':continue
        refs=[r for block in study['blocks'] for r in block.get('references',[])]
        for name in study['aliases']:
            options=candidates(name);profile=choose(options,refs)
            if profile is None:
                profile=dict(key=study['key'],name=name,label=study['title'],aliases=[normalize_term(name)],sections=[],citations=refs,topic_keys=[])
                register(profile)
            profile['sections'].append(dict(title=('Bible Character Studies — ' if study['kind']=='character' else '')+study['title'],
                source_page=study['source_page'],blocks=study['blocks'],pages=study.get('pages',[])))
            if profile['citations'] is not refs:profile['citations'].extend(refs)
    for number,data in material['images'].items():
        conn.execute('''INSERT INTO bible_thompson_study_pages(edition_id,source_page,image_png) VALUES(?,?,?)
            ON CONFLICT(edition_id,source_page) DO UPDATE SET image_png=excluded.image_png''',(edition_id,number,data))
    for p in profiles:
        conn.execute('''INSERT INTO bible_person_profiles(edition_id,person_key,name,label,aliases_json,content_json,citations_json)
            VALUES(?,?,?,?,?,?,?) ON CONFLICT(edition_id,person_key) DO UPDATE SET label=excluded.label,
            aliases_json=excluded.aliases_json,content_json=excluded.content_json,citations_json=excluded.citations_json''',
            (edition_id,p['key'],p['name'],p['label'],json.dumps(p['aliases']),
             json.dumps(dict(sections=p['sections'],topic_keys=list(dict.fromkeys(p['topic_keys'])),context_required=p.get('context_required',False)),ensure_ascii=False),json.dumps(p['citations'])))
    if profiles:
        placeholders=','.join('?' for _ in profiles)
        conn.execute('DELETE FROM bible_person_profiles WHERE edition_id=? AND person_key NOT IN ('+placeholders+')',
                     (edition_id,*[p['key'] for p in profiles]))


def _person_payload(conn,row):
    content=json.loads(row['content_json'])
    topics=[]
    for key in content.get('topic_keys',[]):
        target=conn.execute('SELECT id,topic_key,title FROM bible_chain_topics WHERE edition_id=? AND topic_key=?',(row['edition_id'],key)).fetchone()
        if target:topics.append(dict(target))
    return dict(id=row['id'],name=row['name'],label=row['label'],sections=content['sections'],topics=topics)


def person_study(conn,term,reference=None):
    if conn.execute('PRAGMA user_version').fetchone()[0]<6:return dict(people=[],selected=None)
    term=normalize_term(re.sub(r"['\u2019]s$",'',term,flags=re.I))
    rows=conn.execute('''SELECT p.* FROM bible_person_profiles p JOIN bible_editions e ON e.id=p.edition_id
        JOIN bible_import_runs r ON r.id=e.import_run_id WHERE e.is_active=1 AND e.status='ready' AND r.status='validated'
        AND EXISTS(SELECT 1 FROM json_each(p.aliases_json) WHERE value=?) ORDER BY p.id''',(term,)).fetchall()
    matches=[]
    if reference:
        book,chapter,verse=reference
        matches=[row for row in rows if any(ref['book_id']==book and (ref['chapter'],ref['verse'])<=(chapter,verse)<=
            (ref['end_chapter'],ref['end_verse']) for ref in json.loads(row['citations_json']))]
    selected=matches[0] if len(matches)==1 else rows[0] if len(rows)==1 and not json.loads(rows[0]['content_json']).get('context_required') else None
    return dict(people=[dict(id=row['id'],name=row['name'],label=row['label']) for row in rows],
                selected=_person_payload(conn,selected) if selected else None)


def import_thompson_concordance(database_path, pdf_path, *, backup_directory=None, dry_run=False, include_studies=False):
    """Add the concordance from the exact indexed PDF without replacing reader records."""
    import pypdfium2 as pdfium
    path = _database_path(database_path)
    data = Path(pdf_path).read_bytes()
    lock = _sync_lock(path, create=True)
    if lock is None:
        raise SyncError('A Bible Study sync is already running.')
    try:
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as conn:
            row = conn.execute('''SELECT e.id FROM bible_editions e JOIN bible_sources s ON s.id=e.source_id
                JOIN bible_edition_details d ON d.edition_id=e.id JOIN bible_import_runs r ON r.id=e.import_run_id
                WHERE e.is_active=1 AND e.status='ready' AND r.status='validated'
                AND d.source_kind='thompson_pdf' AND s.sha256=?''',
                (hashlib.sha256(data).hexdigest(),)).fetchone()
            if not row:
                raise SyncError('The PDF must match the currently indexed Thompson source exactly.')
            edition_id = row[0]
        with _PDFIUM_LOCK, pdfium.PdfDocument(data) as pdf:
            entries = _parse_thompson_concordance(pdf)
            material = _parse_thompson_studies(pdf) if include_studies else None
        with tempfile.TemporaryDirectory(prefix='district4_concordance_') as directory:
            staged = Path(directory)/'validation.db'
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as source, closing(sqlite3.connect(staged)) as target:
                source.backup(target)
            initialize_database(staged)
            with closing(sqlite3.connect(staged)) as conn, conn:
                _publish_thompson_concordance(conn, edition_id, entries)
                if material:_publish_thompson_studies(conn,edition_id,material)
                if conn.execute('PRAGMA integrity_check').fetchone()[0]!='ok' or conn.execute('PRAGMA foreign_key_check').fetchall():
                    raise SyncError('Concordance validation failed; the existing database was kept.')
        report = dict(entries=len(entries),excerpts=sum(b.get('role')=='excerpt' for e in entries for b in e['blocks']),dry_run=dry_run)
        if material:report.update(book_studies=66,character_studies=sum(s['kind']=='character' for s in material['studies']),classifications=len(material['classifications']))
        if dry_run:
            return report
        directory=Path(backup_directory) if backup_directory else path.parent/'backups'
        directory.mkdir(parents=True,exist_ok=True)
        backup=directory/('bible_study_before_'+('studies_' if material else 'concordance_')+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_')+secrets.token_hex(3)+'.db')
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as source, closing(sqlite3.connect(backup)) as target:
            source.backup(target)
            if target.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
                raise SyncError('Backup validation failed; the existing database was kept.')
        initialize_database(path)
        with closing(sqlite3.connect(path.as_uri()+'?mode=rw',uri=True)) as conn, conn:
            conn.execute('PRAGMA foreign_keys=ON')
            _publish_thompson_concordance(conn, edition_id, entries)
            if material:_publish_thompson_studies(conn,edition_id,material)
        return dict(report,backup=str(backup))
    finally:
        _release_sync_lock(lock)


def parse_thompson_pdf(data, progress=None):
    """Import the inspected 2022 NIV 2011 Thompson layout, never OCR/guess verses.

    Unsupported editions fail validation. Readers use SQLite only, never this PDF.
    """
    if len(data)>128*1024*1024:
        raise SyncError('The Thompson PDF exceeds the supported size limit.')
    if not data.startswith(b'%PDF-'):
        raise SyncError('The Thompson source is not a readable PDF.')
    try:
        import pypdfium2 as pdfium
        from pypdf import PdfReader
    except ImportError:
        raise SyncError('Install the Bible Study PDF dependencies from requirements.txt before syncing Thompson.') from None
    with _PDFIUM_LOCK:
        reader = PdfReader(io.BytesIO(data))
        if reader.is_encrypted or not 2000<=len(reader.pages)<=2200:
            raise SyncError('Expected the inspected, unencrypted NIV 2011 Thompson edition.')
        copyright_text=reader.pages[4].extract_text() or ''
        if not all(term in copyright_text for term in ('2011','2022','Thompson')):
            raise SyncError('The Thompson PDF edition could not be verified.')
        contents=reader.pages[5].extract_text() or ''
        starts=[]
        for line in contents.splitlines():
            match=re.match(r'^\s*([1-3]?\s*[A-Za-z ]+),.*?(\d+)\s*$',line)
            if match:
                book=BOOK_BY_NAME.get(_text(match[1]).casefold())
                if book:
                    starts.append((book[0],int(match[2])+20))  # zero-based PDF page, printed page + 21
        if [b for b,_ in starts]!=list(range(1,67)):
            raise SyncError('The Thompson table of contents is incomplete or unsupported.')
        verses, topics, warnings = {}, {}, []
        current=None
        pending_heading=[]
        chars=[]
        chapter=0
        def finish():
            nonlocal chars
            if current and chars:
                text, spans=_thompson_verse_text(chars)
                if text:
                    if current in verses:
                        raise SyncError('The Thompson PDF repeats a verse boundary; existing text was kept.')
                    verses[current]=dict(text=text,heading=' '.join(pending_for_verse),red_spans=spans,
                        source_page=verse_page,poetry=current[0] in (18,19,20,21,22))
            chars=[]
        pending_for_verse=[]
        verse_page=0
        with pdfium.PdfDocument(data) as pdf:
            for ordinal,(book,start_page) in enumerate(starts):
                finish();current=None;chapter=0;pending_heading=[];deferred_chapter=None
                end_page=starts[ordinal+1][1] if ordinal+1<len(starts) else 1504
                if book==39:
                    end_page=1150  # the following OT/NT comparison chart is not scripture
                if progress:
                    progress('Indexing Thompson: '+BOOKS[book-1][0]+'.')
                for page_index in range(start_page,end_page):
                    page=pdf[page_index]
                    runs=[]
                    kind=None
                    try:
                        for glyph in _thompson_glyphs(page):
                            char=glyph['char']
                            if char.isspace():
                                if runs and kind:
                                    runs[-1][1].append((char,runs[-1][1][-1][1]))
                                continue
                            font,size=glyph['font'],glyph['size']
                            category=None
                            if 68<glyph['y']<680:
                                if 'ZNIVSerifClosed11-Regular' in font and 18<size<29 and char.isdigit():
                                    category='chapter'
                                elif 7.5<size<8.2:
                                    if 'ZNIVSansClosed11-Italic' in font and char.isdigit():
                                        category='verse'
                                    elif 'ZNIVSerifClosed11-Italic' in font:
                                        category='text'
                                elif 8.3<size<10.5:
                                    if 'ZNIVSansClosed11-Regular' in font and char.isdigit():
                                        category='verse'
                                    elif 'ZNIVSerifClosed11-Regular' in font:
                                        category='text'
                                    elif 'ZNIVSerifClosed11-Semibold' in font or 'ZNIVSerifClosed11-Italic' in font:
                                        category='heading'
                            if category:
                                if not runs or kind!=category:
                                    runs.append([category,[]])
                                runs[-1][1].append((char,book>=40 and glyph['red']))
                            kind=category
                    finally:
                        page.close()
                    for kind,run in runs:
                        label=_pdf_clean(''.join(c for c,_ in run))
                        if kind=='heading':
                            psalm=re.search(r'\bPsalm\s+(\d+)\b',label)
                            if book==19 and psalm:
                                finish();chapter=int(psalm[1]);current=None
                                pending_heading=[]
                            elif label:
                                pending_heading.append(label)
                        elif kind=='chapter':
                            # In this printed edition the large John 8 numeral
                            # precedes 7:53, on the same baseline as 8:1. The
                            # explicit verse markers determine the transition.
                            if book==43 and chapter==7 and int(label)==8:
                                deferred_chapter=8;continue
                            finish();chapter=int(label);current=(book,chapter,1)
                            _reference(*current)
                            pending_for_verse=pending_heading;pending_heading=[];verse_page=page_index+1
                        elif kind=='verse':
                            number=int(label)
                            if deferred_chapter and number==1:
                                chapter=deferred_chapter;deferred_chapter=None
                            if chapter==0 and BOOKS[book-1][1]==1:
                                chapter=1
                            if not chapter:
                                continue
                            reference=_reference(book,chapter,number)
                            if reference!=current:
                                finish();current=reference
                                pending_for_verse=pending_heading;pending_heading=[];verse_page=page_index+1
                        elif kind=='text' and current:
                            chars.extend(run)
            finish()
            topics = _parse_thompson_index(pdf, progress)
            concordance = _parse_thompson_concordance(pdf, progress)
            studies = _parse_thompson_studies(pdf, starts, progress)
        content=dict(verses=verses,topics=topics,concordance=concordance,studies=studies,thompson=True,edition_label='NIV 2011 · Thompson',
            attribution='Scripture taken from the Holy Bible, NEW INTERNATIONAL VERSION®, NIV®. Copyright © 1973, 1978, 1984, 2011 by Biblica, Inc. Thompson Chain-Reference system © 1983, 2022.',warnings=warnings)
        _validate_niv_source(content)
        if not 31080<=len(verses)<=31110:
            raise SyncError('Thompson verse-count validation failed; the existing reader was kept.')
        if len(topics)<4000 or not {'884','2201','2729'}.issubset(topics):
            raise SyncError('Thompson chain-topic validation failed; the existing reader was kept.')
        if not verses.get((19,1,1),{}).get('text','').startswith('Blessed is the one'):
            raise SyncError('Thompson NIV 2011 text verification failed.')
        if not verses.get((40,5,44),{}).get('red_spans'):
            raise SyncError('Thompson red-letter verification failed.')
        return content


def _text(value):
    return re.sub(r'\s+', ' ', value or '').strip()


def _tag(node):
    return node.tag.rsplit('}', 1)[-1].lower()


def _epub_target(filename, href):
    parts = urlsplit(href)
    if parts.scheme or parts.netloc or parts.query:
        return ''
    path = posixpath.normpath(posixpath.join(posixpath.dirname(filename), unquote(parts.path))) if parts.path else filename
    if path.startswith(('/', '../')) or '\\' in path:
        return ''
    return path + ('#' + unquote(parts.fragment) if parts.fragment else '')


def _xml(data):
    if b'<!ENTITY' in data.upper():
        raise SyncError('EPUB entity declarations are not supported.')
    try:
        # Resolve known HTML entities locally; never load an external DTD.
        text = data.decode('utf-8-sig')
        def entity(match):
            if match[1] in {'amp', 'lt', 'gt', 'apos', 'quot'}:
                return match[0]
            value = html.entities.html5.get(match[1] + ';')
            return ''.join('&#' + str(ord(char)) + ';' for char in value) if value else match[0]
        return ET.fromstring(re.sub(r'&([A-Za-z][A-Za-z0-9]+);', entity, text))
    except ET.ParseError:
        raise SyncError('An EPUB contains invalid XML. No active Bible data was replaced.') from None


def _epub_documents(data):
    """Read the declared spine in memory; never extract paths or execute source markup."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise SyncError('The selected source is not a readable EPUB.') from None
    with archive:
        entries = archive.infolist()
        if (len(entries) > 25000 or len({entry.filename for entry in entries}) != len(entries)
                or sum(entry.file_size for entry in entries) > 512 * 1024 * 1024
                or any(entry.file_size > 64 * 1024 * 1024 or entry.flag_bits & 1 for entry in entries)):
            raise SyncError('EPUB archive exceeds supported limits or has ambiguous/encrypted entries.')
        try:
            container = _xml(archive.read('META-INF/container.xml'))
            package_name = next(node.attrib.get('full-path', '') for node in container.iter() if _tag(node) == 'rootfile')
            if not _epub_target('', package_name) or '#' in package_name:
                raise SyncError('Invalid EPUB package path.')
            package = _xml(archive.read(package_name))
            manifest = {node.attrib['id']: node.attrib for node in package.iter() if _tag(node) == 'item'}
            names = []
            for node in package.iter():
                if _tag(node) != 'itemref':
                    continue
                item = manifest[node.attrib['idref']]
                name = _epub_target(package_name, item['href'])
                if not name or '#' in name or name in names:
                    raise SyncError('Invalid or repeated EPUB reading-order entry.')
                names.append(name)
            if not names:
                raise SyncError('The EPUB has no supported reading order.')
            for name in names:
                yield name, archive.read(name)
        except (KeyError, StopIteration, RuntimeError, zipfile.BadZipFile):
            raise SyncError('The EPUB package or a reading-order document could not be read.') from None


def _reference(book, chapter, verse):
    if not (1 <= book <= 66 and 1 <= chapter <= BOOKS[book - 1][1] and 1 <= verse <= 176):
        raise SyncError('The source contains an invalid verse reference.')
    return book, chapter, verse


# Dictionary citations use their own abbreviation conventions. In particular,
# 'Phil.' means Philippians here; it is not the legacy suite's Philemon label.
_REFERENCE_ALIASES = (
    'Gen Ge Gn|Exod Exo Ex|Lev Le Lv|Num Nu Nm|Deut Deu Dt|Josh Jos|Judg Jdg Jdgs|Ruth Ru Rth|'
    '1 Sam 1 Sa 1 Sm|2 Sam 2 Sa 2 Sm|1 Kings 1 Kgs 1 Ki|2 Kings 2 Kgs 2 Ki|1 Chr 1 Chron 1 Ch|2 Chr 2 Chron 2 Ch|'
    'Ezra Ezr|Neh Ne|Esth Est|Job Jb|Ps Psa Pss Psalm|Prov Pro Pr|Eccl Ecc Eccle|Song Song of Solomon Song of Songs SS Sos|'
    'Isa Is|Jer Je|Lam La|Ezek Eze Ezk|Dan Da Dn|Hos Ho|Joel Joe Jl|Amos Amo Am|Obad Oba Ob|Jonah Jon|Mic Mi|Nah Na|Hab|'
    'Zeph Zep|Hag|Zech Zec|Mal|Matt Mt Mat|Mark Mk Mar|Luke Lk Lu Luk|John Jn Jhn Joh|Acts Ac Act|Rom Ro Rm|'
    '1 Cor 1 Co|2 Cor 2 Co|Gal Ga|Eph Ep|Phil Php Phi|Col|1 Thess 1 Thes 1 Th 1Th|2 Thess 2 Thes 2 Th 2Th|'
    '1 Tim 1 Ti|2 Tim 2 Ti|Titus Tit|Philem Phlm Phm|Heb He|Jas James Jam|1 Pet 1 Pe 1 Pt|2 Pet 2 Pe 2 Pt|'
    '1 John 1 Jn 1 Jo 1Jn|2 John 2 Jn 2 Jo 2Jn|3 John 3 Jn 3 Jo 3Jn|Jude Jud|Rev Re Rv'
).split('|')
# Multiword aliases above are added explicitly; ordinary abbreviations are tokens.
REFERENCE_BOOKS = {}
for _index, (_name, _chapters) in enumerate(BOOKS, 1):
    REFERENCE_BOOKS[_name] = _index
    _aliases = _REFERENCE_ALIASES[_index - 1]
    if _index in (9, 10, 11, 12, 13, 14, 46, 47, 52, 53, 54, 55, 60, 61, 62, 63, 64):
        _items = re.findall(r'[1-3]\s*[A-Za-z]+', _aliases)
    elif _index == 22:
        _items = ['Song', 'Song of Solomon', 'Song of Songs', 'SS', 'Sos', 'Son']
    else:
        _items = _aliases.split()
    for _alias in _items:
        REFERENCE_BOOKS[_alias] = _index
_canonical_reference_keys={re.sub(r'[.\s]+','',name).casefold() for name in REFERENCE_BOOKS}
for _alias,_index in list(REFERENCE_BOOKS.items()):
    if re.match(r'^[1-3]\s*[A-Za-z]',_alias):
        _roman_alias=('I','II','III')[int(_alias[0])-1]+' '+_alias[1:].lstrip()
        # "I Sa" must not replace Isaiah's existing "Isa" abbreviation.
        if re.sub(r'[.\s]+','',_roman_alias).casefold() not in _canonical_reference_keys:
            REFERENCE_BOOKS[_roman_alias]=_index
_REF_NAMES = {re.sub(r'[.\s]+', '', name).casefold(): book for name, book in REFERENCE_BOOKS.items()}
_BOOK_PATTERN = '|'.join(re.escape(name).replace(r'\ ', r'\s*') for name in sorted(REFERENCE_BOOKS, key=len, reverse=True))
_RANGE_PATTERN = r'(?P<chapter>\d{1,3}):(?P<verse>\d{1,3})[a-c]?(?:\s*[-–—]\s*(?:(?P<end_chapter>\d{1,3}):)?(?P<end_verse>\d{1,3})[a-c]?)?'
SCRIPTURE_PATTERN = re.compile(r'(?<![\w])(?P<book>' + _BOOK_PATTERN + r')\.?\s*' + _RANGE_PATTERN + r'(?![\w:])', re.I)
_INHERITED_REFERENCE = re.compile(r'\s*[,;]\s*(?:(?P<chapter>\d{1,3}):)?(?P<verse>\d{1,3})[a-c]?(?:\s*[-–—]\s*(?:(?P<end_chapter>\d{1,3}):)?(?P<end_verse>\d{1,3})[a-c]?)?(?![\w:])')
_SINGLE_BOOKS = '|'.join(re.escape(name).replace(r'\ ', r'\s*') for name in sorted(REFERENCE_BOOKS, key=len, reverse=True)
                         if BOOKS[REFERENCE_BOOKS[name]-1][1]==1)
_SINGLE_CHAPTER_REFERENCE = re.compile(r'(?<![\w])(?P<book>'+_SINGLE_BOOKS+r')\.?\s+(?P<verse>\d{1,3})[a-c]?(?:\s*[-–—]\s*(?P<last>\d{1,3})[a-c]?)?(?![\w:])', re.I)


def scripture_references(text):
    """Parse explicit citations and immediately adjacent inherited references.

    A citation records what a dictionary cites, never an NIV word alignment.
    Offsets use Unicode code points, like the Bible reader.
    """
    result, consumed = [], 0
    for match in SCRIPTURE_PATTERN.finditer(text):
        if match.start() < consumed:
            continue
        book = _REF_NAMES[re.sub(r'[.\s]+', '', match['book']).casefold()]
        chapter = int(match['chapter'])
        current = match
        while current:
            chapter = int(current['chapter']) if current['chapter'] else chapter
            verse = int(current['verse'])
            end_chapter = int(current['end_chapter']) if current['end_chapter'] else chapter
            end_verse = int(current['end_verse']) if current['end_verse'] else verse
            if not (1 <= chapter <= end_chapter <= BOOKS[book-1][1] and 1 <= verse <= 176 and 1 <= end_verse <= 176
                    and (chapter, verse) <= (end_chapter, end_verse)):
                break
            start = current.start()
            if current is not match:
                start += len(re.match(r'\s*[,;]\s*', current[0])[0])
            result.append(dict(start=start, end=current.end(), book=BOOKS[book-1][0], book_id=book,
                               chapter=chapter, verse=verse, end_chapter=end_chapter, end_verse=end_verse,
                               source_text=text[start:current.end()]))
            consumed = current.end()
            current = _INHERITED_REFERENCE.match(text, consumed)
            # A free-standing number followed by prose may be a statistic.
            if current and re.match(r'\s+[A-Za-z]', text[current.end():]):
                current = None
    for match in _SINGLE_CHAPTER_REFERENCE.finditer(text):
        if any(match.start()<item['end'] and match.end()>item['start'] for item in result):
            continue
        book = _REF_NAMES[re.sub(r'[.\s]+','',match['book']).casefold()]
        verse, last = int(match['verse']), int(match['last'] or match['verse'])
        if 1<=verse<=last<=176:
            result.append(dict(start=match.start(), end=match.end(), book=BOOKS[book-1][0], book_id=book,
                               chapter=1, verse=verse, end_chapter=1, end_verse=last, source_text=match[0]))
    return sorted(result, key=lambda item:item['start'])


def normalize_term(value):
    return ' '.join(''.join(char for char in unicodedata.normalize('NFKD', value.casefold())
                            if not unicodedata.combining(char)).split()).strip(' .,:;()')


def _dictionary_block(element, filename, archive, assets):
    runs = []
    def walk(node, marks=()):
        tag = _tag(node)
        if tag in {'script', 'style', 'iframe', 'object', 'svg'}:
            return
        if tag == 'img':
            target = _epub_target(filename, node.attrib.get('src', ''))
            if not target or '#' in target:
                raise SyncError('A dictionary image has an unsupported source path.')
            if target not in assets:
                try:
                    data = archive.read(target)
                    from PIL import Image
                    with Image.open(io.BytesIO(data)) as picture:
                        mime = Image.MIME.get(picture.format)
                        if mime not in {'image/gif', 'image/jpeg', 'image/png'} or max(picture.size) > 4096 or len(data) > 2 * 1024 * 1024:
                            raise SyncError('A dictionary image exceeds supported limits.')
                        picture.verify()
                except (KeyError, OSError, ValueError):
                    raise SyncError('A dictionary image could not be validated.') from None
                assets[target] = dict(sha256=hashlib.sha256(data).hexdigest(), mime_type=mime, data=data)
            runs.append(dict(type='image', asset=assets[target]['sha256'], marks=list(marks)))
            return
        mark = {'i': 'em', 'em': 'em', 'b': 'strong', 'strong': 'strong', 'sup': 'sup', 'sub': 'sub'}.get(tag)
        if mark:
            marks = (*marks, mark)
        if node.text:
            runs.append(dict(type='text', text=re.sub(r'\s+', ' ', node.text), marks=list(marks)))
        for child in node:
            walk(child, marks)
            if child.tail:
                runs.append(dict(type='text', text=re.sub(r'\s+', ' ', child.tail), marks=list(marks)))
        if tag == 'br':
            runs.append(dict(type='text', text='\n', marks=list(marks)))
    walk(element)
    while runs and runs[0]['type'] == 'text' and not runs[0]['text'].strip():
        runs.pop(0)
    while runs and runs[-1]['type'] == 'text' and not runs[-1]['text'].strip():
        runs.pop()
    if runs and runs[0]['type'] == 'text':
        runs[0]['text'] = runs[0]['text'].lstrip()
    if runs and runs[-1]['type'] == 'text':
        runs[-1]['text'] = runs[-1]['text'].rstrip()
    plain = ''.join(run['text'] if run['type'] == 'text' else '\ufffc' for run in runs)
    kind = 'heading' if _tag(element) in {'h1', 'h2', 'h3', 'h4'} or element.attrib.get('class') in {'h1', 'h2', 'pc', 'bh'} else 'paragraph'
    return dict(type=kind, runs=runs, text=plain, references=scripture_references(plain))


def _entry_terms(entry):
    terms = {(normalize_term(entry['headword']), 'headword', entry['headword'], 1)}
    if entry['section'] == 'Expository dictionary':
        for word in re.findall(r"[A-Za-z]+(?:['’][A-Za-z]+)?", entry['headword']):
            terms.add((normalize_term(word), 'headword', word, 1))
    for block in entry['blocks']:
        # Only explicitly italicized lexical forms become transliteration terms;
        # incomplete image-based spellings stay marked incomplete.
        for run in block['runs']:
            if run['type'] == 'text' and 'em' in run['marks']:
                value = _text(run['text']).strip(' ,;:.()')
                if value and len(value) < 90:
                    terms.add((normalize_term(value), 'transliteration', value,
                               int(not any(item['type'] == 'image' for item in block['runs']))))
        for original in re.findall(r'[\u0370-\u03ff\u1f00-\u1fff\u0590-\u05ff]+', block['text']):
            terms.add((normalize_term(original), 'original', original, 1))
    return sorted(term for term in terms if term[0])


def _entry_identifiers(entry):
    identifiers = set()
    language = entry['language']
    for block in entry['blocks']:
        text = block['text']
        if text in {'Old Testament', 'New Testament'}:
            language = 'Hebrew' if text == 'Old Testament' else 'Greek'
        if language not in {'Hebrew', 'Greek'}:
            continue
        if entry['family'] == 'mounce':
            for match in re.finditer(r'\bGK\s*(\d+)\s*\(S\s*([^)]*)\)', text):
                identifiers.add(('gk', language, int(match[1]), '', match[0]))
                for number in re.finditer(r'(\d+)(\*\*?|[dv])?', match[2]):
                    if int(number[1]) > 0:
                        identifiers.add(('strong', language, int(number[1]), number[2] or '', match[0]))
            if entry['section'] != 'Expository dictionary':
                first = re.match(r'^\[(\d+)\]', text)
                if first and int(first[1]) > 0:
                    identifiers.add(('gk', language, int(first[1]), '', first[0]))
                    # Source instructions identify the final numeric brackets as
                    # Strong's numbers, including qualified and multiple forms.
                    for bracket in re.finditer(r'\[([\d ,&*dv]+)\]', text[first.end():]):
                        for number in re.finditer(r'(\d+)(\*\*?|[dv])?', bracket[1]):
                            if int(number[1]) > 0:
                                identifiers.add(('strong', language, int(number[1]), number[2] or '', bracket[0]))
        else:
            # A numbered lexical definition starts with italicized transliteration
            # followed by its identifier, not arbitrary numbers elsewhere in prose.
            for match in re.finditer(r'\((\d{1,4}(?:\*\*?|[dv])?(?:\s*(?:,|and|&)\s*\d{1,4}(?:\*\*?|[dv])?)*)\)', text):
                position, lexical = 0, False
                for run in block['runs']:
                    size = len(run['text']) if run['type']=='text' else 1
                    position += size
                    if (run['type']=='text' and 'em' in run['marks'] and 0<len(run['text'].strip())<80
                            and position<=match.start() and not text[position:match.start()].strip()):
                        lexical = True
                if not lexical:
                    continue
                for number in re.finditer(r'(\d+)(\*\*?|[dv])?', match[1]):
                    if int(number[1]) > 0:
                        suffix = re.match(r'(\*\*?|[dv])', text[match.end():])
                        qualifier = number[2] or (suffix[0] if suffix else '')
                        identifiers.add(('strong', language, int(number[1]), qualifier, match[0]+(suffix[0] if suffix else '')))
    return sorted(identifiers)


def dictionary_senses(entry):
    """Definition boundaries and identifiers explicitly printed by each source.

    A whole English article can discuss several Hebrew/Greek words. Keep those
    definitions separate; a nearby citation does not align an NIV token.
    """
    blocks=entry['blocks']; language=entry['language']; starts=[]
    for index,block in enumerate(blocks):
        text=block['text']
        if text in ('Old Testament','New Testament'):
            language='Hebrew' if text=='Old Testament' else 'Greek'
        lexical=False
        if entry['family']=='mounce':
            lexical=(entry['section']!='Expository dictionary' and index==0) or bool(
                re.match(r'^(?:Verb|Noun|Adjective|Adverb|Preposition|Particle|Pronoun|Proper noun)\b[^\n]{0,200}?\bGK\s*\d+\s*\(S\s*',text,re.I))
        else:
            # Require the lexical introduction at the beginning of a paragraph.
            # This excludes italic prose followed by an enumerated (1), etc.
            prefix=re.match(r'^\s*(?:\d+\.?\s*)?[^()]{1,90}\s*\(\d{1,4}(?:[,*dv &\d]|and)*\)',text)
            if prefix:
                offset=0
                for run in block.get('runs',[]):
                    size=len(run.get('text','')) if run['type']=='text' else 1
                    if run['type']=='text' and 'em' in run.get('marks',[]) and offset<prefix.end():
                        before=text[:offset].strip()
                        if not before or re.fullmatch(r'\d+\.?',before):lexical=True
                    offset+=size
        if lexical and language in ('Hebrew','Greek'):
            ids=_entry_identifiers({**entry,'language':language,'blocks':[block]})
            starts.append((index,language,ids))
    if not starts and entry['language'] in ('Hebrew','Greek'):
        starts=[(0,entry['language'],[])]
    for position,(start,language,identifiers) in enumerate(starts):
        end=starts[position+1][0] if position+1<len(starts) else len(blocks)
        # A testament heading is a hard boundary, including when the next
        # testament has no lexical definition that can be indexed reliably.
        for index in range(start+1,end):
            if blocks[index]['text'] in ('Old Testament','New Testament'):
                end=index;break
        yield dict(language=language,block_start=start,block_end=end,identifiers=identifiers)


def rebuild_dictionary_senses(conn):
    for row in conn.execute('''SELECT e.*,d.family FROM bible_dictionary_entries e
        JOIN bible_dictionary_versions d ON d.id=e.version_id
        WHERE NOT EXISTS(SELECT 1 FROM bible_dictionary_senses s WHERE s.entry_id=e.id)''').fetchall():
        columns=[column[0] for column in conn.execute('SELECT e.*,d.family FROM bible_dictionary_entries e JOIN bible_dictionary_versions d ON d.id=e.version_id LIMIT 0').description]
        entry=dict(zip(columns,row));entry['blocks']=json.loads(entry['content_json'])
        for sense in dictionary_senses(entry):
            identifier=conn.execute('''INSERT OR IGNORE INTO bible_dictionary_senses(entry_id,language,block_start,block_end)
                VALUES(?,?,?,?)''',(entry['id'],sense['language'],sense['block_start'],sense['block_end'])).lastrowid
            conn.executemany('''INSERT OR IGNORE INTO bible_dictionary_sense_identifiers
                (sense_id,scheme,language,number,qualifier,source_text) VALUES(?,?,?,?,?,?)''',
                [(identifier,*item) for item in sense['identifiers']])


def english_word_forms(word):
    word=normalize_term(word);forms={word}
    irregular={'loved':'love','loving':'love','children':'child','men':'man','women':'woman',
               'went':'go','gone':'go','gave':'give','given':'give','said':'say','taught':'teach',
               'bought':'buy','brought':'bring','was':'be','were':'be','is':'be','are':'be',
               'has':'have','had':'have','did':'do','done':'do','better':'good','best':'good'}
    if word in irregular:forms.add(irregular[word])
    if word.endswith('ies') and len(word)>4:forms.add(word[:-3]+'y')
    if word.endswith('s') and len(word)>3 and not word.endswith(('ss','us','is')):forms.add(word[:-1])
    if word.endswith('es') and len(word)>4:forms.add(word[:-2])
    for ending in ('ed','ing','ers','er'):
        if word.endswith(ending) and len(word)>len(ending)+2:
            root=word[:-len(ending)];forms.update((root,root+'e'))
            if len(root)>2 and root[-1]==root[-2]:forms.add(root[:-1])
    return sorted(forms)


def _dictionary_preview_block(block,limit=360):
    """Keep original-language image glyphs and emphasis in the short preview."""
    runs=[];used=0;truncated=False
    for run in block.get('runs',[]):
        size=1 if run['type']=='image' else len(run.get('text',''))
        if used+size<=limit:
            runs.append(run);used+=size;continue
        if run['type']=='text' and used<limit:
            runs.append({**run,'text':run['text'][:limit-used]})
        truncated=True;break
    if truncated:runs.append(dict(type='text',text='…',marks=[]))
    return dict(type='paragraph',text=block['text'][:limit]+('…' if truncated else ''),runs=runs,
        references=[r for r in block.get('references',[]) if r['end']<=limit])


def contextual_dictionary_study(conn,token):
    """English dictionary candidates, narrowed by source definition and testament.

    STEP is corroborating verse-reference evidence, never an NIV alignment.
    """
    if conn.execute('PRAGMA user_version').fetchone()[0]<4:
        return dict(verified=False,result_type='unavailable',sources=[],message=NO_VERIFIED_ALIGNMENT)
    language='Hebrew' if token['book_id']<40 else 'Greek'
    forms=english_word_forms(token['text']);placeholders=','.join('?' for _ in forms)
    candidates=conn.execute(f'''SELECT DISTINCT e.*,d.family,s.id AS sense_id,s.block_start,s.block_end,s.language AS sense_language
        FROM bible_dictionary_terms t JOIN bible_dictionary_entries e ON e.id=t.entry_id
        JOIN bible_dictionary_versions d ON d.id=e.version_id
        JOIN bible_import_runs r ON r.id=d.import_run_id
        JOIN bible_dictionary_senses s ON s.entry_id=e.id
        WHERE t.kind='headword' AND t.term IN ({placeholders}) AND s.language=? AND d.is_active=1
        AND r.status='validated' AND (d.family='vine' OR e.section='Expository dictionary') LIMIT 150''',(*forms,language)).fetchall()
    verse_ids={r[0] for r in conn.execute('''SELECT i.standard_strong FROM bible_original_tokens t
        JOIN bible_original_versions v ON v.id=t.version_id JOIN bible_original_token_identifiers i ON i.token_id=t.id
        WHERE v.is_active=1 AND t.book_id=? AND t.chapter=? AND t.verse=?''',
        (token['book_id'],token['chapter'],token['verse']))}
    sources=[]
    for family,label in (('mounce','Mounce'),('vine','Vine')):
        matches=[]
        for entry in candidates:
            if entry['family']!=family:continue
            headwords={normalize_term(part) for part in re.split(r'[,;()]',entry['headword'])}
            if not headwords.intersection(forms):continue
            blocks=json.loads(entry['content_json'])[entry['block_start']:entry['block_end']]
            ids=[dict(r) for r in conn.execute('SELECT scheme,language,number,qualifier,source_text FROM bible_dictionary_sense_identifiers WHERE sense_id=?',(entry['sense_id'],))]
            strongs={(('H' if item['language']=='Hebrew' else 'G')+str(item['number'])) for item in ids if item['scheme']=='strong'}
            common=sorted(strongs & verse_ids)
            cited=any(r['book_id']==token['book_id'] and (r['chapter'],r['verse'])<=(token['chapter'],token['verse'])<=(r['end_chapter'],r['end_verse'])
                for block in blocks for r in block.get('references',[]))
            plain='\n\n'.join(b['text'] for b in blocks);preview=_text(plain.replace('\ufffc',''))[:360]
            evidence=[]
            if cited:evidence.append('Dictionary definition cites this verse')
            if common:evidence.append('STEP identifiers indexed at this verse reference: '+', '.join(common))
            match=dict(id=entry['id'],sense_id=entry['sense_id'],headword=entry['headword'],section=entry['section'],language=language,
                preview=preview,preview_references=scripture_references(preview),identifiers=ids,evidence=evidence,
                preview_block=_dictionary_preview_block(blocks[0]) if blocks else None,
                score=10*bool(common)+3*cited,matching_strongs=common)
            matches.append(match)
        matches.sort(key=lambda x:(-x['score'],x['id'],x['sense_id']))
        # If explicit identifiers corroborate candidates, omit other senses of
        # the same English headword, rather than presenting unrelated definitions.
        supported=[m for m in matches if m['matching_strongs']]
        if supported:matches=supported
        matches=matches[:6]
        if family=='mounce' and matches:
            identifiers={(i['scheme'],i['language'],i['number']) for m in matches for i in m['identifiers']}
            related={}
            for scheme,lang,number in sorted(identifiers):
                for row in conn.execute('''SELECT DISTINCT e.*,s.id AS sense_id FROM bible_dictionary_sense_identifiers i
                    JOIN bible_dictionary_senses s ON s.id=i.sense_id JOIN bible_dictionary_entries e ON e.id=s.entry_id
                    JOIN bible_dictionary_versions d ON d.id=e.version_id JOIN bible_import_runs r ON r.id=d.import_run_id
                    WHERE i.scheme=? AND i.language=? AND i.number=? AND d.family='mounce' AND d.is_active=1
                    AND r.status='validated' AND e.section!='Expository dictionary' LIMIT 4''',(scheme,lang,number)):
                    preview=row['preview']
                    related[row['id']]=dict(id=row['id'],sense_id=row['sense_id'],headword=row['headword'],section=row['section'],language=row['language'],
                        preview=preview,preview_references=scripture_references(preview),identifiers=[],
                        preview_block=_dictionary_preview_block(json.loads(row['content_json'])[0]),
                        evidence=['Supporting dictionary entry linked by explicit '+('Strong’s' if scheme=='strong' else 'GK')+' identifier'])
            matches.extend(list(related.values())[:4])
        sources.append(dict(source=family,label=label,available=bool(conn.execute('SELECT 1 FROM bible_dictionary_versions WHERE family=? AND is_active=1',(family,)).fetchone()),entries=matches,next_offset=None))
    if not any(source['available'] for source in sources):
        return dict(verified=False,result_type='unavailable',sources=[],message=NO_VERIFIED_ALIGNMENT)
    return dict(verified=False,result_type='dictionary',query=token['text'],sources=sources,
        explanation='Dictionary explanations, not a verified NIV word alignment. Verse references and STEP identifiers help narrow the possibilities.')


def parse_dictionary_epub(data, family):
    if family not in {'mounce', 'vine'}:
        raise SyncError('Unsupported dictionary source.')
    entries, assets, current = [], {}, None
    section, language = '', ''
    def finish():
        nonlocal current
        if current:
            current['plain_text'] = '\n\n'.join(block['text'] for block in current['blocks'])
            current['terms'] = _entry_terms(current)
            current['identifiers'] = _entry_identifiers(current)
            entries.append(current)
        current = None
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for filename, content in _epub_documents(data):
            root = _xml(content)
            if family == 'vine':
                if not re.search(r'-[on]t[A-Z]', filename):
                    continue
                language = 'Hebrew' if '-ot' in filename else 'Greek'
                parents = {child: parent for parent in root.iter() for child in parent}
                for ordinal, paragraph in enumerate(node for node in root.iter() if _tag(node) == 'p'):
                    if paragraph.attrib.get('class') == 'bh':
                        finish()
                        parent = parents.get(paragraph)
                        anchor = ''
                        while parent is not None:
                            if parent.attrib.get('id'):
                                anchor = parent.attrib['id']
                                break
                            parent = parents.get(parent)
                        current = dict(family=family, locator=filename+'#'+(anchor or 'p'+str(ordinal)),
                                       headword=_text(''.join(paragraph.itertext())),
                                       section='Expository dictionary', language=language, blocks=[])
                    elif current and paragraph.attrib.get('class') != 'ah':
                        block = _dictionary_block(paragraph, filename, archive, assets)
                        if block['text'].strip():
                            current['blocks'].append(block)
            else:
                for ordinal, paragraph in enumerate(node for node in root.iter() if _tag(node) == 'p'):
                    label = _text(''.join(paragraph.itertext()))
                    cls = paragraph.attrib.get('class', '')
                    if cls in {'h1b', 'toch'} and section in {'Hebrew-English Dictionary', 'Greek-English Dictionary'}:
                        finish()
                        section = ''
                        continue
                    if cls == 'h' and label in {'Hebrew-English Dictionary', 'Greek-English Dictionary'}:
                        finish()
                        section = label
                        language = 'Hebrew' if label.startswith('Hebrew') else 'Greek'
                        continue
                    # The main article section begins at ch5; never index the
                    # introduction's example paragraphs as actual entries.
                    if paragraph.attrib.get('id') == 'ch5':
                        section = 'Expository dictionary'
                    is_main = section == 'Expository dictionary' and cls == 'h2'
                    is_appendix = section in {'Hebrew-English Dictionary', 'Greek-English Dictionary'} and cls == 'tx2' and re.match(r'^\[\d+\]', label)
                    if is_main or is_appendix:
                        finish()
                        headword = label if is_main else re.match(r'^\[\d+\]', label)[0]
                        current = dict(family=family, locator=filename+'#p'+str(ordinal), headword=headword,
                                       section=section, language='' if is_main else language, blocks=[])
                    if current and not is_main:
                        block = _dictionary_block(paragraph, filename, archive, assets)
                        if block['text'].strip():
                            current['blocks'].append(block)
                    if section and paragraph.attrib.get('id') in {'about', 'publisher'}:
                        finish()
                        section = ''
                # Chapter/section state deliberately survives split spine files.
        finish()
    if not entries:
        raise SyncError('No supported ' + family.title() + ' dictionary entries were found.')
    return dict(entries=entries, assets=assets)


def search_dictionaries(conn, query, offset=0, family=None):
    query = _text(query)[:100]
    groups = []
    ready = conn.execute('PRAGMA user_version').fetchone()[0] >= 2
    term = normalize_term(query)
    strong = re.fullmatch(r'([HG])\s*0*(\d+)', query, re.I)
    gk = re.fullmatch(r'GK\s*[: -]?\s*(?:(Hebrew|Greek|H|G)\s*[: -]?\s*)?0*(\d+)', query, re.I)
    for source in ('mounce', 'vine'):
        if family and family != source:
            continue
        group = dict(source=source, label='Mounce' if source=='mounce' else 'Vine', entries=[], next_offset=None, available=False)
        groups.append(group)
        if not ready:
            continue
        version = conn.execute('''SELECT d.id FROM bible_dictionary_versions d JOIN bible_import_runs r ON r.id=d.import_run_id
            WHERE d.family=? AND d.is_active=1 AND r.status='validated' ''', (source,)).fetchone()
        if not version:
            continue
        group['available'] = True
        parameters = [version[0]]
        if strong or gk:
            match = strong or gk
            language = ('Hebrew' if match[1].casefold() in {'h','hebrew'} else 'Greek') if match[1] else None
            where = '''EXISTS(SELECT 1 FROM bible_dictionary_identifiers i WHERE i.entry_id=e.id
                AND i.scheme=? AND i.number=? AND (? IS NULL OR i.language=?))'''
            parameters.extend(['strong' if strong else 'gk', int(match[2]), language, language])
        else:
            variants = [term]
            # English spelling variants only select dictionary headwords, never
            # original-language mappings. Always show the source's actual heading.
            if re.fullmatch('[a-z]{4,}', term):
                if term.endswith('ies'):
                    variants.append(term[:-3]+'y')
                elif term.endswith('s') and not term.endswith('ss'):
                    variants.extend([term[:-1], term[:-2] if term.endswith('es') else term[:-1]])
            variants = list(dict.fromkeys(variants))
            where = 'EXISTS(SELECT 1 FROM bible_dictionary_terms t WHERE t.entry_id=e.id AND t.term IN ('+','.join('?' for _ in variants)+'))'
            parameters.extend(variants)
        rows = conn.execute('''SELECT e.id,e.headword,e.section,e.language,e.preview FROM bible_dictionary_entries e
            WHERE e.version_id=? AND ''' + where + ''' ORDER BY CASE WHEN lower(e.headword)=? THEN 0 ELSE 1 END,
            CASE WHEN EXISTS(SELECT 1 FROM bible_dictionary_terms t WHERE t.entry_id=e.id AND t.term=? AND t.kind='headword') THEN 0 ELSE 1 END,
            CASE WHEN e.section='Expository dictionary' THEN 0 ELSE 1 END,e.id LIMIT 9 OFFSET ?''',
                            (*parameters, term, term, offset)).fetchall()
        group['entries'] = [{**dict(row), 'preview_references': scripture_references(row['preview'])} for row in rows[:8]]
        group['next_offset'] = offset+8 if len(rows)>8 else None
    return dict(query=query, sources=groups, message='Dictionary search results; these are not exact NIV original-language matches.')


STEP_BOOK_IDS = {name: i for i, name in enumerate(('Gen Exo Lev Num Deu Jos Jdg Rut 1Sa 2Sa 1Ki 2Ki '
    '1Ch 2Ch Ezr Neh Est Job Psa Pro Ecc Sng Isa Jer Lam Ezk Dan Hos Jol Amo Oba Jon Mic Nam Hab Zep '
    'Hag Zec Mal Mat Mrk Luk Jhn Act Rom 1Co 2Co Gal Eph Php Col 1Th 2Th 1Ti 2Ti Tit Phm Heb Jas '
    '1Pe 2Pe 1Jn 2Jn 3Jn Jud Rev').split(), 1)}
STEP_FILES = (
    'Lexicons/TBESH - Translators Brief lexicon of Extended Strongs for Hebrew - STEPBible.org CC BY.txt',
    'Lexicons/TBESG - Translators Brief lexicon of Extended Strongs for Greek - STEPBible.org CC BY.txt',
    *(f'Translators Amalgamated OT+NT/TAHOT {part} - Translators Amalgamated Hebrew OT - STEPBible.org CC BY.txt'
      for part in ('Gen-Deu', 'Jos-Est', 'Job-Sng', 'Isa-Mal')),
    *(f'Translators Amalgamated OT+NT/TAGNT {part} - Translators Amalgamated Greek NT - STEPBible.org CC-BY.txt'
      for part in ('Mat-Jhn', 'Act-Rev')),
)
STEP_IDENTIFIER = re.compile(r'[HG]\d+[A-Za-z]*')
STEP_MIN_VERSES = (23000, 7900)
STEP_REFERENCE = re.compile(r'^([1-3]?[A-Za-z]{2,3})\.(\d+)\.(\d+)(?:[a-z])?(?:(?:\([^)]*\)|\[[^\]]*\]|\{[^}]*\}))*#(\d+)(.*)$')
NO_VERIFIED_ALIGNMENT = 'No verified original-language equivalent is available for this word in this verse.'


def _step_manifest(archive):
    """Read only the eight current source files. Never extract or execute ZIP content."""
    names = archive.namelist()
    if len(names) != len(set(names)) or len(names) > 2000:
        raise SyncError('STEPBible ZIP contains duplicate paths or too many files.')
    selected = []
    for relative in STEP_FILES:
        matches = [name for name in names if name.endswith('/' + relative) or name == relative]
        if len(matches) != 1:
            raise SyncError('STEPBible ZIP must contain exactly one current copy of ' + relative)
        info = archive.getinfo(matches[0])
        if info.file_size > 64 * 1024 * 1024 or info.flag_bits & 1:
            raise SyncError('STEPBible source is oversized or encrypted.')
        selected.append(info.filename)
    if sum(archive.getinfo(name).file_size for name in selected) > 256 * 1024 * 1024:
        raise SyncError('STEPBible source files exceed the import size limit.')
    return selected


def _step_lines(archive, name):
    with archive.open(name) as raw, io.TextIOWrapper(raw, encoding='utf-8-sig', errors='strict') as stream:
        for number, line in enumerate(stream, 1):
            if len(line) > 512 * 1024:
                raise SyncError('STEPBible contains an unexpectedly long record.')
            yield number, line.rstrip('\r\n').split('\t')


def _step_standard_identifier(estrong):
    # STEP documents dStrong suffixes as subdivisions of the standard number.
    # Never use uStrong here: it can cross languages/people. STEP-only additions
    # outside the historical Strong ranges must not become standard IDs.
    match = re.fullmatch(r'([HG])(\d+)[A-Za-z]*', estrong)
    if match and 1 <= int(match[2]) <= (8674 if match[1] == 'H' else 5624):
        return match[1] + str(int(match[2]))
    return ''


def _step_link_dictionaries(conn):
    """Lexical associations only, never evidence for an NIV token alignment."""
    conn.execute('''INSERT OR IGNORE INTO bible_original_dictionary_links
        (lexeme_id,entry_id,standard_strong,match_method)
        SELECT l.id,i.entry_id,l.standard_strong,'explicit_standard_strong'
        FROM bible_original_lexemes l JOIN bible_original_versions o ON o.id=l.version_id
        JOIN bible_dictionary_identifiers i ON i.scheme='strong' AND i.qualifier=''
          AND l.standard_strong=(CASE i.language WHEN 'Hebrew' THEN 'H' ELSE 'G' END)||i.number
        JOIN bible_dictionary_entries e ON e.id=i.entry_id
        JOIN bible_dictionary_versions d ON d.id=e.version_id
        JOIN bible_import_runs r ON r.id=d.import_run_id
        WHERE o.is_active=1 AND d.is_active=1 AND r.status='validated' ''')


def step_import_report(conn, version_id):
    counts = dict(conn.execute('''SELECT CASE WHEN book_id<=39 THEN 'ot_verses' ELSE 'nt_verses' END,COUNT(*)
        FROM (SELECT DISTINCT book_id,chapter,verse FROM bible_original_tokens WHERE version_id=?)
        GROUP BY book_id<=39''', (version_id,)).fetchall())
    counts['source_records'] = conn.execute('SELECT COUNT(*) FROM bible_original_tokens WHERE version_id=?', (version_id,)).fetchone()[0]
    counts['total_tokens'] = conn.execute("SELECT COUNT(*) FROM bible_original_tokens WHERE version_id=? AND token_kind='word'", (version_id,)).fetchone()[0]
    counts['omission_markers'] = counts['source_records'] - counts['total_tokens']
    counts['tokens_by_language'] = dict(conn.execute("SELECT language,COUNT(*) FROM bible_original_tokens WHERE version_id=? AND token_kind='word' GROUP BY language", (version_id,)))
    counts['lexemes'] = conn.execute('SELECT COUNT(*) FROM bible_original_lexemes WHERE version_id=?', (version_id,)).fetchone()[0]
    counts['extended_identifiers'] = conn.execute('''SELECT COUNT(DISTINCT i.extended_strong)
        FROM bible_original_token_identifiers i JOIN bible_original_tokens t ON t.id=i.token_id WHERE t.version_id=?''', (version_id,)).fetchone()[0]
    counts['standard_identifiers'] = conn.execute('''SELECT COUNT(DISTINCT i.standard_strong)
        FROM bible_original_token_identifiers i JOIN bible_original_tokens t ON t.id=i.token_id
        WHERE t.version_id=? AND i.standard_strong!='' ''', (version_id,)).fetchone()[0]
    counts['identifiers_without_lexeme'] = conn.execute('''SELECT COUNT(DISTINCT i.extended_strong)
        FROM bible_original_token_identifiers i JOIN bible_original_tokens t ON t.id=i.token_id
        WHERE t.version_id=? AND i.lexeme_id IS NULL''', (version_id,)).fetchone()[0]
    counts['dictionary_links_by_source'] = dict(conn.execute('''SELECT d.family,COUNT(*)
        FROM bible_original_dictionary_links x JOIN bible_original_lexemes l ON l.id=x.lexeme_id
        JOIN bible_dictionary_entries e ON e.id=x.entry_id JOIN bible_dictionary_versions d ON d.id=e.version_id
        WHERE l.version_id=? AND d.is_active=1 GROUP BY d.family''', (version_id,)))
    counts['dictionary_links'] = sum(counts['dictionary_links_by_source'].values())
    counts['new_niv_alignments'] = 0
    return counts


def _load_step_archive(conn, zip_path, digest):
    """Transactional, versioned source import. Caller controls commit/rollback."""
    with Path(zip_path).open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != digest:
            raise SyncError('STEPBible archive changed during validation. Restart the import.')
    existing = conn.execute('''SELECT o.id,o.is_active FROM bible_original_versions o
        JOIN bible_sources s ON s.id=o.source_id WHERE s.sha256=?''', (digest,)).fetchone()
    if existing:
        if not existing[1]:
            raise SyncError('This older STEPBible archive is retained but inactive. Refusing to reactivate it automatically.')
        _step_link_dictionaries(conn)
        return dict(status='unchanged', counts=step_import_report(conn, existing[0]))
    now = datetime.now(timezone.utc).isoformat()
    with zipfile.ZipFile(zip_path) as archive:
        names = _step_manifest(archive)
        manifest = []
        for name in names:
            with archive.open(name) as stream:
                sha = hashlib.file_digest(stream, 'sha256').hexdigest()
            manifest.append(dict(file=name, sha256=sha, bytes=archive.getinfo(name).file_size, license='CC BY 4.0'))
        source_id = conn.execute('''INSERT INTO bible_sources(title,edition,sha256,source_path,attribution,rights_note,created_at)
            VALUES('STEPBible original-language datasets','TAHOT/TAGNT/TBESH/TBESG',?,?,?,?,?)''',
            (digest, str(zip_path), 'STEP Bible / Tyndale House, Cambridge; https://github.com/STEPBible/STEPBible-Data',
             'CC BY 4.0. Source data preserved; HTML definitions also stored as plain text for display.', now)).lastrowid
        run_id = conn.execute("INSERT INTO bible_import_runs(source_id,status,started_at) VALUES(?,'staged',?)", (source_id, now)).lastrowid
        version_id = conn.execute('''INSERT INTO bible_original_versions(source_id,import_run_id,manifest_json)
            VALUES(?,?,?)''', (source_id, run_id, json.dumps(manifest))).lastrowid
        lexemes = {}
        for name in names[:2]:
            for line, columns in _step_lines(archive, name):
                if not re.match(r'^[HG]\d', columns[0]):
                    continue
                if len(columns) < 8:
                    raise SyncError('STEPBible lexicon record is incomplete.')
                match = STEP_IDENTIFIER.match(columns[1])
                if not match or match[0] in lexemes:
                    raise SyncError('STEPBible lexicon contains an invalid or duplicate identifier.')
                identifier = match[0]
                standard = _step_standard_identifier(columns[0])
                language = 'Aramaic' if columns[5].startswith('A:') else ('Hebrew' if identifier.startswith('H') else 'Greek')
                definition = _text(unescape_html(re.sub(r'<[^>]*>', ' ', columns[7])))
                lexeme_id = conn.execute('''INSERT INTO bible_original_lexemes
                    (version_id,extended_strong,standard_strong,source_estrong,unified_strong,language,
                     lemma,transliteration,morphology,gloss,definition,source_file,source_line,raw_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (version_id, identifier, standard, columns[0], columns[2], language, columns[3], columns[4],
                     columns[5], columns[6], definition, name, line, json.dumps(columns, ensure_ascii=False))).lastrowid
                lexemes[identifier] = (lexeme_id, standard, columns[3])
        for name in names[2:]:
            greek = '/TAGNT ' in name
            file_tokens = 0
            for line, columns in _step_lines(archive, name):
                if not re.match(r'^[1-3]?[A-Za-z]{2,3}\.\d+\.\d+.*#', columns[0]):
                    continue  # Headers and repeated interlinear summaries are not tokens.
                match = STEP_REFERENCE.fullmatch(columns[0])
                if not match or match[1] not in STEP_BOOK_IDS or len(columns) < (6 if greek else 12):
                    raise SyncError('STEPBible token reference or columns are unsupported.')
                book, chapter, verse, position = STEP_BOOK_IDS[match[1]], int(match[2]), int(match[3]), int(match[4])
                if (book > 39) != greek:
                    raise SyncError('STEPBible token is in the wrong testament source.')
                if greek:
                    spelling_match = re.fullmatch(r'(.*?)\s+\(([^()]*)\)', columns[1])
                    spelling, transliteration = spelling_match.groups() if spelling_match else (columns[1], '')
                    tags, _, morphology = columns[3].partition('=')
                    lemma = columns[4].partition('=')[0]
                    gloss, witnesses, language = columns[2], columns[5], 'Greek'
                else:
                    spelling, transliteration, gloss, tags, morphology = columns[1:6]
                    witnesses, language = '', 'Aramaic' if morphology.startswith('A') else 'Hebrew'
                    roots = re.findall(r'\{([^}]+)\}', tags)
                    lemma = ' / '.join(dict.fromkeys(lexemes[m[0]][2] for root in roots
                        for m in STEP_IDENTIFIER.finditer(root) if m[0] in lexemes))
                token_id = conn.execute('''INSERT INTO bible_original_tokens
                    (version_id,book_id,chapter,verse,token_position,source_ref,language,spelling,token_kind,transliteration,
                     lemma,morphology,gloss,text_type,witnesses,source_file,source_line,raw_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (version_id, book, chapter, verse, position, columns[0], language, spelling, 'word' if spelling else 'omission', transliteration,
                     lemma, morphology, gloss, match[5], witnesses, name, line, json.dumps(columns, ensure_ascii=False))).lastrowid
                identifiers = list(dict.fromkeys(m[0] for m in STEP_IDENTIFIER.finditer(tags)))
                for ordinal, identifier in enumerate(identifiers):
                    lexeme = lexemes.get(identifier)
                    conn.execute('''INSERT INTO bible_original_token_identifiers
                        (token_id,ordinal,extended_strong,standard_strong,lexeme_id,role) VALUES(?,?,?,?,?,?)''',
                        (token_id, ordinal, identifier, lexeme[1] if lexeme else _step_standard_identifier(identifier), lexeme[0] if lexeme else None,
                         'root' if greek or '{'+identifier+'}' in tags else 'component'))
                file_tokens += 1
            if not file_tokens:
                raise SyncError('STEPBible source file contains no supported tokens.')
        # Every version retains its own IDs. Never delete prior tokens or alignment records.
        conn.execute('UPDATE bible_original_versions SET is_active=0 WHERE is_active=1')
        conn.execute('UPDATE bible_original_versions SET is_active=1 WHERE id=?', (version_id,))
        _step_link_dictionaries(conn)
        counts = step_import_report(conn, version_id)
        books = conn.execute('SELECT COUNT(DISTINCT book_id) FROM bible_original_tokens WHERE version_id=?', (version_id,)).fetchone()[0]
        if books != 66 or counts.get('ot_verses', 0) < STEP_MIN_VERSES[0] or counts.get('nt_verses', 0) < STEP_MIN_VERSES[1]:
            raise SyncError('STEPBible validation requires coverage of all 66 books and both testaments.')
        for book, chapter, verse, strong in ((6, 1, 1, 'H3091'), (58, 4, 8, 'G2424'), (43, 3, 16, 'G25')):
            if not conn.execute('''SELECT 1 FROM bible_original_tokens t JOIN bible_original_token_identifiers i ON i.token_id=t.id
                WHERE t.version_id=? AND t.book_id=? AND t.chapter=? AND t.verse=? AND i.standard_strong=?''',
                (version_id, book, chapter, verse, strong)).fetchone():
                raise SyncError('STEPBible validation failed a required reference/identifier check.')
        if conn.execute('PRAGMA foreign_key_check').fetchone():
            raise SyncError('STEPBible import failed foreign-key validation.')
        report = dict(status='completed', counts=counts, archive_sha256=digest,
            attribution='STEP Bible / Tyndale House, Cambridge (CC BY 4.0)',
            notes=['Token totals include source manuscript variants; omission markers are counted separately.',
                   'Passage coordinates retain STEPBible primary NRSV versification; alternate references remain in source_ref.',
                   'Extended identifiers without an exact accompanying lexicon entry are retained without a guessed lemma or definition.',
                   'Dictionary links associate lexical identifiers, not NIV word alignments or a guaranteed contextual sense.'])
        conn.execute("UPDATE bible_import_runs SET status='validated',finished_at=?,report_json=? WHERE id=?", (now, json.dumps(report), run_id))
        with Path(zip_path).open('rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != digest:
                raise SyncError('STEPBible archive changed while being read. Imported records were rolled back.')
        conn.execute('UPDATE bible_original_versions SET report_json=? WHERE id=?', (json.dumps(report), version_id))
        return report


def _step_preserved_state(conn, baseline=None):
    """Hash pre-existing rows, including annotations and NIV codepoint positions."""
    names = baseline or {row[0]: None for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'bible_original_%'")}
    result = {}
    for name in names:
        if not re.fullmatch(r'bible_[a-z_]+', name):
            raise SyncError('Unexpected table in Bible Study database.')
        limit = baseline[name][0] if baseline else (conn.execute(f'SELECT COALESCE(MAX(rowid),0) FROM {name}').fetchone()[0]
            if name in ('bible_sources', 'bible_import_runs') else None)
        sql = f'SELECT * FROM {name}' + (' WHERE rowid<=?' if limit is not None else '') + ' ORDER BY rowid'
        digest, count = hashlib.sha256(), 0
        for row in conn.execute(sql, (limit,) if limit is not None else ()):
            digest.update(repr(tuple(row)).encode('utf-8')); count += 1
        result[name] = (limit, count, digest.hexdigest())
    return result


def import_step_archive(database_path, archive_path, *, backup_directory=None, dry_run=False):
    """Validate on a SQLite snapshot first, back up, then import atomically.

    No app.py import, network requests, English-gloss alignments or data cleanup.
    """
    path = _database_path(database_path)
    archive_path = Path(archive_path).expanduser().resolve(strict=True)
    if not path.is_file() or not archive_path.is_file():
        raise ValueError('An existing Bible Study database and STEPBible ZIP are required.')
    with archive_path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    lock = _sync_lock(path, create=True)
    if lock is None:
        raise SyncError('Bible Study sync is already running. Try again after it completes.')
    try:
        with tempfile.TemporaryDirectory(prefix='district4_step_validation_') as directory:
            staged = Path(directory) / 'bible_study.db'
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as source:
                if source.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID:
                    raise ValueError('Refusing to import into an unrecognized database.')
                with closing(sqlite3.connect(staged)) as target:
                    source.backup(target)
            with closing(sqlite3.connect(staged)) as conn:
                before = _step_preserved_state(conn)
            initialize_database(staged)
            with closing(sqlite3.connect(staged)) as conn:
                conn.execute('PRAGMA foreign_keys=ON')
                with conn:
                    temporary_report = _load_step_archive(conn, archive_path, digest)
                    if _step_preserved_state(conn, before) != before:
                        raise SyncError('Validation detected changes to existing Bible Study records.')
                if conn.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                    raise SyncError('Temporary Bible Study database failed integrity validation.')
            if dry_run:
                return dict(**temporary_report, temporary_validation='passed', applied=False)
        # Back up the live database BEFORE schema migration, with SQLite's backup
        # API so WAL contents are included. Never copy/replace a live DB file.
        backup_directory = Path(backup_directory or (path.parent.parent if path.parent.name=='bible_study_data' else path.parent) / 'bible_study_backups').resolve()
        if backup_directory == path:
            raise ValueError('Invalid backup directory.')
        backup_directory.mkdir(parents=True, exist_ok=True)
        backup = backup_directory / ('bible_study_before_step_' + datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_') + secrets.token_hex(3) + '.db')
        _database_path(backup)
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as source:
            with closing(sqlite3.connect(backup)) as target:
                source.backup(target)
                if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                    raise SyncError('Bible Study backup failed integrity validation.')
        initialize_database(path)
        with closing(sqlite3.connect(path.as_uri()+'?mode=rw', uri=True, timeout=30)) as conn:
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('BEGIN IMMEDIATE')
            try:
                before = _step_preserved_state(conn)
                report = _load_step_archive(conn, archive_path, digest)
                if report['counts'] != temporary_report['counts'] or _step_preserved_state(conn, before) != before:
                    raise SyncError('Live import validation failed. Imported records were rolled back.')
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return dict(**report, temporary_validation='passed', applied=True, backup=str(backup))
    finally:
        _release_sync_lock(lock)


def verified_word_study(conn, token):
    """Fail closed: original verse words/glosses are not NIV alignment evidence."""
    empty = dict(verified=False, message=NO_VERIFIED_ALIGNMENT, original_tokens=[], sources=[])
    if conn.execute('PRAGMA user_version').fetchone()[0] < 3:
        return empty
    verse = conn.execute('SELECT text FROM bible_verses WHERE id=?', (token['verse_id'],)).fetchone()
    if not verse or hashlib.sha256(verse[0].encode('utf-8')).hexdigest() != token['content_hash']:
        return empty
    alignments = conn.execute('''SELECT a.* FROM bible_niv_alignments a
        JOIN bible_niv_alignment_tokens m ON m.alignment_id=a.id
        WHERE m.token_id=? AND a.verse_id=? AND a.status='verified' AND a.verse_content_hash=?''',
        (token['id'], token['verse_id'], token['content_hash'])).fetchall()
    originals, evidence = {}, []
    for alignment in alignments:
        members = conn.execute('''SELECT t.*,v.text AS verse_text FROM bible_niv_alignment_tokens m
            JOIN bible_tokens t ON t.id=m.token_id JOIN bible_verses v ON v.id=t.verse_id
            WHERE m.alignment_id=?''', (alignment['id'],)).fetchall()
        if not members or any(t['verse_id'] != token['verse_id'] or
                t['verse_text'][t['start_offset']:t['end_offset']] != t['text'] for t in members):
            continue
        rows = conn.execute('''SELECT t.*,o.is_active,r.status FROM bible_niv_alignment_originals m
            JOIN bible_original_tokens t ON t.id=m.original_token_id
            JOIN bible_original_versions o ON o.id=t.version_id JOIN bible_import_runs r ON r.id=o.import_run_id
            WHERE m.alignment_id=? ORDER BY t.token_position,t.id''', (alignment['id'],)).fetchall()
        if not rows or any(not t['is_active'] or t['status']!='validated' or t['token_kind']!='word' or
                (t['book_id'],t['chapter'],t['verse']) != (token['book_id'],token['chapter'],token['verse']) for t in rows):
            continue
        for row in rows:
            item = {key: row[key] for key in ('id','language','spelling','transliteration','lemma','morphology','source_ref')}
            item['identifiers'] = [dict(r) for r in conn.execute('''SELECT i.extended_strong,i.standard_strong,l.definition,l.lemma,l.transliteration
                FROM bible_original_token_identifiers i LEFT JOIN bible_original_lexemes l ON l.id=i.lexeme_id
                WHERE i.token_id=? ORDER BY i.ordinal''', (row['id'],))]
            originals[row['id']] = item
        evidence.append(dict(source_id=alignment['evidence_source_id'], locator=alignment['evidence_locator'], method=alignment['verification_method']))
    if not originals:
        return empty
    groups = []
    placeholders = ','.join('?' for _ in originals)
    for family in ('mounce','vine'):
        entries = conn.execute('''SELECT DISTINCT e.id,e.headword,e.section,e.language,e.preview
            FROM bible_original_token_identifiers i JOIN bible_original_dictionary_links x ON x.lexeme_id=i.lexeme_id
            JOIN bible_dictionary_entries e ON e.id=x.entry_id JOIN bible_dictionary_versions d ON d.id=e.version_id
            JOIN bible_import_runs r ON r.id=d.import_run_id WHERE i.token_id IN (''' + placeholders + ''')
            AND d.family=? AND d.is_active=1 AND r.status='validated' ORDER BY e.id''', (*originals, family)).fetchall()
        groups.append(dict(source=family, label='Mounce' if family=='mounce' else 'Vine', available=bool(conn.execute(
            'SELECT 1 FROM bible_dictionary_versions WHERE family=? AND is_active=1', (family,)).fetchone()),
            entries=[{**dict(e),'preview_references':scripture_references(e['preview'])} for e in entries], next_offset=None))
    return dict(verified=True, message='Verified original-language alignment.', original_tokens=list(originals.values()), sources=groups, evidence=evidence)


KJV_ABBREVIATIONS = ('Gen Exo Lev Num Deu Jos Jdg Rut 1Sa 2Sa 1Ki 2Ki 1Ch 2Ch Ezr Neh Est Job Psa Pro Ecc '
                     'Son Isa Jer Lam Ezek Dan Hos Joe Amo Oba Jon Mic Nah Hab Zep Hag Zec Mal Mat Mar Luk Joh Act '
                     'Rom 1Co 2Co Gal Eph Phi Col 1Th 2Th 1Ti 2Ti Tit Phm Heb Jam 1Pet 2Pet 1Jo 2Jo 3Jo Jude Rev').split()
KJV_BOOKS = {name.casefold(): index for index, name in enumerate(KJV_ABBREVIATIONS, 1)}
KJV_BOOKS.update({re.sub(r'\s+', '', name).casefold(): index for index, (name, _) in enumerate(BOOKS, 1)})
KJV_BOOKS.update({'ps': 19, 'psalm': 19, 'song': 22, '1jn': 62, '2jn': 63, '3jn': 64, 'phil': 57,
                  'philem': 57, '1cor': 46, '2cor': 47, '1sam': 9, '2sam': 10, '1kgs': 11, '2kgs': 12,
                  'deut': 5, 'josh': 6, 'judg': 7, '1kng': 11, '2kng': 12, '1chr': 13, '2chr': 14,
                  'prov': 20, 'obad': 31, 'zeph': 36, 'zech': 38, '1the': 52, '2the': 53,
                  '1tim': 54, '2tim': 55, '1jhn': 62, '2jhn': 63, '3jhn': 64})


def parse_niv_epub(data):
    verses, current, heading = {}, None, ''
    parts = []

    def finish():
        nonlocal current, parts
        if current is not None:
            text = _text(''.join(parts))
            if not text or current in verses:
                raise SyncError('NIV verse markers are empty or duplicated; sync was stopped.')
            verses[current] = {'text': text, 'heading': heading, 'anchor': current_anchor}
        current, parts = None, []

    def walk(node, filename):
        nonlocal current, current_anchor, heading
        tag = _tag(node)
        classes = set(node.attrib.get('class', '').split())
        marker = re.search(r'htmlv(\d{2})(\d{3})(\d{3})$', node.attrib.get('id', ''))
        if marker:
            finish()
            current = _reference(*(int(value) for value in marker.groups()))
            current_anchor = filename + '#' + node.attrib['id']
        if tag in {'head', 'script', 'style', 'sup', 'img'} or 'fn' in classes:
            return
        if classes & {'ah', 'bh', 'ch', 'cn', 'ct', 'toc1', 'toc2', 'toc3'}:
            finish()
            heading = _text(''.join(node.itertext())) if classes & {'ah', 'bh'} else ''
            return
        if tag == 'a' and ('rfn' in node.attrib.get('id', '') or 'htmlfn' in node.attrib.get('href', '')):
            return
        if current and node.text:
            parts.append(node.text)
        for child in node:
            walk(child, filename)
            if current and child.tail:
                parts.append(child.tail)
        if current and tag in {'p', 'br', 'div'}:
            parts.append(' ')

    current_anchor = ''
    for filename, content in _epub_documents(data):
        # Footnotes/back matter do not form part of the last Scripture verse.
        if b'htmlv' not in content:
            finish()
            continue
        walk(_xml(content), filename)
    finish()
    return {'verses': verses, 'lexicon': {}, 'links': [], 'red_spans': []}


def parse_suite_epub(data):
    verses, lexicon, links, red_spans, lemmas = {}, {}, [], [], {}
    language, anchor = '', ''
    for filename, content in _epub_documents(data):
        # Concordance occurrences are reconstructed from explicit KJV-to-dictionary
        # hyperlinks, not from reference ranges or English similarity guesses.
        if b'color="green"' not in content and b"Strong's {" not in content and b'Strong\xe2\x80\x99s {' not in content and b'Dictionary' not in content:
            continue
        root = _xml(content)
        for node in root.iter():
            if _tag(node) == 'a' and node.attrib.get('href'):
                label = _text(''.join(node.itertext()))
                target = _epub_target(filename, node.attrib['href'])
                if target and label and len(label) < 90 and re.search(r'[A-Za-z\u0370-\u03ff\u0590-\u05ff]', label):
                    lemmas.setdefault(target, label)
            if _tag(node) in {'h1', 'h2'}:
                label = _text(''.join(node.itertext())).upper()
                if 'GREEK DICTIONARY' in label:
                    language = 'Greek'
                elif 'HEBREW' in label and 'DICTIONARY' in label:
                    language = 'Hebrew'
            if _tag(node) != 'p':
                continue
            for child in node.iter():
                if child.attrib.get('id'):
                    anchor = filename + '#' + child.attrib['id']
            label = _text(''.join(node.itertext()))
            definition = re.match(r"\{([^}]+)\}\s*Strong['’]s\s*\{(\d+)\}", label)
            if definition:
                if not language or not anchor.startswith(filename + '#') or 'Definitions:' not in label:
                    raise SyncError('A Strong\'s entry lacks an explicit language, anchor or definition.')
                strong_id = ('H' if language == 'Hebrew' else 'G') + str(int(definition[2]))
                lexicon[anchor] = {'strong_id': strong_id, 'language': language, 'pronunciation': definition[1],
                                   'definition': label.split('Definitions:', 1)[1].strip(), 'anchor': anchor}
                continue
            reference_node = next((child for child in node.iter() if _tag(child) == 'font'
                                   and child.attrib.get('color', '').lower() == 'green'), None)
            if reference_node is None:
                continue
            match = re.fullmatch(r'([1-3]?\s*[A-Za-z]+)\s+(\d+):(\d+)', _text(''.join(reference_node.itertext())))
            if not match:
                raise SyncError('Unsupported KJV verse marker. No active data was replaced.')
            book = KJV_BOOKS.get(re.sub(r'\s+', '', match[1]).casefold())
            if book is None:
                raise SyncError('Unsupported KJV book abbreviation: ' + match[1])
            key = _reference(book, int(match[2]), int(match[3]))
            if key in verses:
                raise SyncError('Duplicate KJV verse marker. No active data was replaced.')
            parts, verse_links, red_parts = [], [], []
            last_link_end = 0

            def walk(element, red=False):
                nonlocal last_link_end
                if element is reference_node or _tag(element) in {'script', 'style', 'img'}:
                    return
                red = red or (element.attrib.get('color', '').lower() == 'red' and key[0] >= 40)
                if _tag(element) == 'a' and element.attrib.get('href'):
                    target = _epub_target(filename, element.attrib['href'])
                    if target:
                        phrase = _text(''.join(parts)[last_link_end:]).strip(' ,;:.!?')
                        if phrase:
                            verse_links.append((target, phrase))
                        last_link_end = len(''.join(parts))
                if element.text:
                    parts.append(element.text)
                    red_parts.append((element.text, red))
                for child in element:
                    walk(child, red)
                    if child.tail:
                        parts.append(child.tail)
                        red_parts.append((child.tail, red))

            walk(node)
            text = _text(''.join(parts))
            if not text:
                raise SyncError('Empty KJV verse in the source.')
            verses[key] = {'text': text, 'heading': '', 'anchor': anchor}
            links.extend((key, target, phrase, anchor) for target, phrase in verse_links)
            # KJV red-letter evidence stays attached to KJV. Never project it
            # onto different NIV wording by verse number or English similarity.
            if any(red for _, red in red_parts):
                red_spans.append(key)
    for entry in lexicon.values():
        entry['lemma'] = lemmas.get(entry['anchor'], '')
    resolved = [link for link in links if link[1] in lexicon]
    return {'verses': verses, 'lexicon': lexicon, 'links': resolved, 'red_spans': red_spans,
            'unresolved_links': len(links) - len(resolved)}


def validate_sources(niv, suite):
    for name, parsed in (('NIV', niv), ('KJV', suite)):
        for reference in parsed['verses']:
            _reference(*reference)
        chapters = {(book, chapter) for book, chapter, verse in parsed['verses']}
        if len(chapters) != 1189 or len(parsed['verses']) < 30000 or (1, 1, 1) not in parsed['verses'] or (66, 22, 21) not in parsed['verses']:
            raise SyncError(name + ' validation failed: expected all 66 books and 1,189 chapters. Existing data was kept.')
    if (sum(entry['language'] == 'Hebrew' for entry in suite['lexicon'].values()) < 8000
            or sum(entry['language'] == 'Greek' for entry in suite['lexicon'].values()) < 5000
            or len(suite['links']) < 100000):
        raise SyncError('Strong\'s dictionary or verse-link coverage is incomplete. Existing data was kept.')


def _library_sources(database_path):
    """Resolve current catalog IDs read-only, without library migrations or scans."""
    path = Path(database_path).resolve()
    if not path.is_file():
        raise SyncError('The existing ebook library database is unavailable.')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('''SELECT f.name,f.drive_file_id,f.size,COALESCE(NULLIF(TRIM(b.manual_title),''),b.title) AS title
            FROM pastor_library_files f JOIN pastor_library_books b ON b.id=f.book_id
            WHERE f.is_active=1 AND f.is_duplicate=0 AND b.is_active=1 AND UPPER(f.format)='EPUB' ''').fetchall()
    selected = {}
    for key, predicate in (
        ('NIV', lambda name: re.fullmatch(r'holy bible\s*\(niv\)\.epub', name, re.I)),
        ('KJV', lambda name: name.casefold().startswith('ultimate bible study suite')),
    ):
        matches = [dict(row) for row in rows if predicate(row['name'].strip())]
        if len(matches) != 1 or not matches[0]['drive_file_id']:
            raise SyncError('Expected exactly one active ' + key + ' source EPUB in the library. Check missing or duplicate source books.')
        selected[key] = matches[0]
    if selected['NIV']['drive_file_id'] == selected['KJV']['drive_file_id']:
        raise SyncError('The two source books must be different EPUBs.')
    return selected


def _usfm_plain(text):
    text = re.sub(r'\\ref\s+([^|\\]+)(?:\|[^\\]*)?\\ref\*', r'\1', text)
    text = re.sub(r'\\[+a-zA-Z][a-zA-Z0-9-]*\*?[ \t]*', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return re.sub(r'\n\s*\n+', '\n', text).strip()


def parse_usfm_archive(data, translation, *, require_complete=True):
    """Parse the supplied publisher archives locally; never OCR or call an AI."""
    if translation not in ('CEBOCB', 'TCB'):
        raise SyncError('Unsupported USFM translation.')
    result, seen_books = [], set()
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = [item for item in archive.infolist() if item.filename.lower().endswith('.usfm')]
            if len(members) > 66 or sum(item.file_size for item in members) > 64 * 1024 * 1024:
                raise SyncError('The USFM archive exceeds the supported size.')
            for member in members:
                text = archive.read(member).decode('utf-8-sig')
                identity = re.search(r'\\id\s+(\w+)', text)
                if not identity or identity[1] not in USFM_BOOK_IDS or identity[1] in seen_books:
                    raise SyncError('A USFM book identifier is missing, unsupported or duplicated.')
                code = identity[1]; seen_books.add(code); book_id = USFM_BOOK_IDS.index(code) + 1
                if len(re.findall(r'\\f\s', text)) != text.count('\\f*'):
                    raise SyncError('A USFM footnote is incomplete.')
                chapters = list(re.finditer(r'\\c\s+(\d+)\s*', text))
                if require_complete and [int(c[1]) for c in chapters] != list(range(1, BOOKS[book_id-1][1]+1)):
                    raise SyncError('USFM chapters are missing or out of order in ' + code + '.')
                for ci, chapter_marker in enumerate(chapters):
                    chapter = int(chapter_marker[1])
                    if not 1 <= chapter <= BOOKS[book_id-1][1]:
                        raise SyncError('Invalid USFM chapter.')
                    body = text[chapter_marker.end():chapters[ci+1].start() if ci+1 < len(chapters) else len(text)]
                    verses = list(re.finditer(r'\\v\s+(\d+(?:-\d+)?)\s+', body))
                    pending = body[:verses[0].start()] if verses else ''
                    previous = 0
                    for vi, marker in enumerate(verses):
                        numbers = marker[1].split('-'); start, end = int(numbers[0]), int(numbers[-1])
                        if not previous < start <= end <= 176 or (require_complete and start != previous + 1):
                            raise SyncError('USFM verse numbers overlap or have gaps in ' + code + '.')
                        previous = end
                        content = body[marker.end():verses[vi+1].start() if vi+1 < len(verses) else len(body)]
                        headings = re.findall(r'(?m)^\\(?:s[1-3]?|ms[1-3]?|d)\s+([^\n]*)', pending)
                        notes = []
                        def footnote(match):
                            notes.append(dict(text=_usfm_plain(re.sub(r'^\\f\s+\S+\s*', '', match[0][:-3]))))
                            return ''
                        content = re.sub(r'\\f\s+.*?\\f\*', footnote, content, flags=re.S)
                        pending = '\n'.join(re.findall(r'(?m)^\\(?:s[1-3]?|ms[1-3]?|d)\s+[^\n]*', content))
                        content = re.sub(r'(?m)^\\(?:s[1-3]?|ms[1-3]?|mr|r|d|cl)\s+[^\n]*\n?', '', content)
                        poetry = bool(re.search(r'\\q[mrc]?[1-4]?\b', content))
                        plain = _usfm_plain(content)
                        if not plain:
                            raise SyncError('An empty USFM verse was found.')
                        result.append(dict(book_id=book_id, chapter=chapter, verse=start, verse_end=end,
                            text=plain, heading=' · '.join(_usfm_plain(h) for h in headings),
                            notes=notes, layout='poetry' if poetry else ''))
                    if not verses:
                        raise SyncError('An empty USFM chapter was found.')
    except (zipfile.BadZipFile, UnicodeError, RuntimeError) as exc:
        raise SyncError('The Bible ZIP is damaged or is not UTF-8 USFM.') from exc
    if not result or (require_complete and seen_books != set(USFM_BOOK_IDS)):
        raise SyncError('The USFM archive must contain all 66 Bible books.')
    return dict(translation=translation, verses=result, books=len(seen_books),
                chapters=len({(v['book_id'],v['chapter']) for v in result}),
                verse_ranges=sum(v['verse_end']>v['verse'] for v in result))


def _publish_usfm(conn, parsed, sources):
    now = datetime.now(timezone.utc).isoformat()
    counts = {}
    for translation, content in parsed.items():
        source = sources[translation]; digest = source['sha256']; edition = READER_TRANSLATIONS[translation]
        key = translation + ':' + digest
        existing = conn.execute('SELECT id FROM bible_editions WHERE edition_key=?', (key,)).fetchone()
        conn.execute('UPDATE bible_editions SET is_active=0 WHERE translation=?', (translation,))
        if existing:
            conn.execute('UPDATE bible_editions SET is_active=1 WHERE id=?', (existing[0],))
        else:
            conn.execute('''INSERT OR IGNORE INTO bible_sources(title,edition,sha256,source_path,attribution,rights_note,created_at)
                VALUES(?,?,?,?,?,?,?)''', (edition['title'],translation,digest,source.get('source_path',''),
                edition['attribution'],'Bible Study reader only. No AI access or AI indexing.',now))
            source_id = conn.execute('SELECT id FROM bible_sources WHERE sha256=?', (digest,)).fetchone()[0]
            run_id = conn.execute("INSERT INTO bible_import_runs(source_id,status,started_at,finished_at) VALUES(?,'validated',?,?)", (source_id,now,now)).lastrowid
            edition_id = conn.execute('''INSERT INTO bible_editions(source_id,import_run_id,edition_key,translation,title,status,is_active,attribution)
                VALUES(?,?,?,?,?,'ready',1,?)''', (source_id,run_id,key,translation,edition['title'],edition['attribution'])).lastrowid
            for verse in content['verses']:
                verse_id = conn.execute('''INSERT INTO bible_verses
                    (edition_id,book_id,chapter,verse,verse_end,text,heading,content_hash,notes_json,layout)
                    VALUES(?,?,?,?,?,?,?,?,?,?)''', (edition_id,verse['book_id'],verse['chapter'],verse['verse'],verse['verse_end'],
                    verse['text'],verse['heading'],hashlib.sha256(verse['text'].encode()).hexdigest(),json.dumps(verse['notes'],ensure_ascii=False),verse['layout'])).lastrowid
                conn.executemany('''INSERT INTO bible_tokens(verse_id,token_index,start_offset,end_offset,text,normalized_text)
                    VALUES(?,?,?,?,?,?)''', ((verse_id,i,word.start(),word.end(),word[0],unicodedata.normalize('NFKC',word[0]).casefold())
                    for i,word in enumerate(re.finditer(r"[^\W\d_]+(?:['’ʼ-][^\W\d_]+)*",verse['text'],re.UNICODE))))
        counts[translation] = len(content['verses'])
    return counts


def import_usfm_translations(database_path, archives, *, sources=None, progress=lambda message: None):
    """Validate a staged copy and back up before publishing either translation."""
    path = _database_path(database_path)
    parsed, records = {}, {}
    for translation, value in archives.items():
        data = value if isinstance(value, bytes) else Path(value).read_bytes()
        progress('Validating ' + READER_TRANSLATIONS[translation]['label'] + ' USFM.')
        parsed[translation] = parse_usfm_archive(data, translation)
        records[translation] = dict((sources or {}).get(translation, {}), sha256=hashlib.sha256(data).hexdigest())
    if not parsed:
        raise SyncError('No translation ZIPs were supplied.')
    if path.exists():
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as conn:
            unchanged = all(conn.execute('''SELECT 1 FROM bible_editions e JOIN bible_import_runs r
                ON r.id=e.import_run_id WHERE e.edition_key=? AND e.is_active=1 AND e.status='ready'
                AND r.status='validated' ''', (kind+':'+source['sha256'],)).fetchone()
                for kind, source in records.items())
        if unchanged:
            return dict(status='completed', counts={kind:len(value['verses']) for kind,value in parsed.items()},
                        backup=None, message='Cebuano/Tagalog ZIPs are unchanged; keeping their indexed editions.')
    with tempfile.TemporaryDirectory(prefix='district4_usfm_validation_') as directory:
        staged = Path(directory)/'validation.db'
        if path.exists():
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as source, closing(sqlite3.connect(staged)) as target:
                source.backup(target)
        initialize_database(staged)
        with closing(sqlite3.connect(staged)) as conn, conn:
            counts = _publish_usfm(conn, parsed, records)
            if conn.execute('PRAGMA foreign_key_check').fetchone() or conn.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise SyncError('Translation database validation failed.')
    backup = None
    if path.exists():
        directory = path.parent/'backups'; directory.mkdir(parents=True,exist_ok=True)
        backup = directory/('before_translations_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_')+secrets.token_hex(3)+'.db')
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as source, closing(sqlite3.connect(backup)) as target:
            source.backup(target)
    initialize_database(path)
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute('PRAGMA foreign_keys=ON')
        published = _publish_usfm(conn, parsed, records)
        if published != counts or conn.execute('PRAGMA foreign_key_check').fetchone():
            raise SyncError('Translation publication failed validation; the recovery backup was kept.')
    return dict(status='completed',counts=counts,backup=str(backup) if backup else None,
                message='Cebuano/Tagalog translations imported into Bible Study only.')


def sync_translation_sources(path, folder_id, progress, *, drive=None, fetch=None):
    owned = drive is None
    drive = drive or resources.get_drive_session()
    try:
        selected = {}
        for item in resources.list_drive_folder(drive, folder_id):
            name = str(item.get('name') or '')
            if not name.casefold().endswith('.zip'): continue
            kind = 'CEBOCB' if 'cebocb' in name.casefold() else 'TCB' if 'tagalog' in name.casefold() else ''
            if not kind: continue
            if kind in selected: raise SyncError('Keep only one current ZIP per translation in the translation folder.')
            selected[kind] = dict(drive_file_id=item['id'],size=item.get('size',0),name=name,
                                  source_path='drive:'+item['id'])
        if not selected: raise SyncError('No Cebuano or Tagalog USFM ZIPs were found in the translation folder.')
        archives = {kind: fetch(source) if fetch else _download_source(drive,source) for kind,source in selected.items()}
        return import_usfm_translations(path,archives,sources=selected,progress=progress)
    finally:
        if owned: drive.close()


def _download_source(drive, source):
    limit = 96 * 1024 * 1024
    file_id = str(source['drive_file_id'])
    if not re.fullmatch(r'[A-Za-z0-9_-]{10,200}', file_id) or int(source.get('size') or 0) > limit:
        raise SyncError('A source file is invalid or exceeds the supported download size.')
    data = bytearray()
    with drive.get('https://www.googleapis.com/drive/v3/files/' + file_id,
                   params={'alt': 'media', 'supportsAllDrives': 'true'}, timeout=(15, 120), stream=True) as response:
        response.raise_for_status()
        for chunk in response.iter_content(1024 * 1024):
            data.extend(chunk)
            if len(data) > limit:
                raise SyncError('An EPUB download exceeds the supported size.')
    return bytes(data)


def _sync_lock(path, create=False):
    """OS lock shared by Gunicorn workers; released automatically after a crash."""
    lock_path = path.with_suffix('.sync.lock')
    if not create and not lock_path.exists():
        return None
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open('a+b')
    if handle.seek(0, 2) == 0:
        handle.write(b'0')
        handle.flush()
    handle.seek(0)
    try:
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        handle.close()
        if error.errno in (11, 13):
            return None
        raise
    return handle


def _release_sync_lock(handle):
    if handle is None:
        return
    try:
        handle.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _write_sync_status(path, report):
    with closing(sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=15)) as conn, conn:
        conn.execute("INSERT INTO bible_metadata(key,value) VALUES('sync_status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (json.dumps(report, ensure_ascii=False),))


def _publish_sources(path, sources, parsed):
    """Publish both sources in one transaction. Never delete old verse revisions."""
    now = datetime.now(timezone.utc).isoformat()
    counts = {key.lower() + '_verses': len(value['verses']) for key, value in parsed.items()}
    counts.update(strongs_entries=len(parsed['KJV']['lexicon']),
                  source_word_links=len(parsed['KJV']['links']), verified_niv_mappings=0,
                  unresolved_source_links=parsed['KJV'].get('unresolved_links', 0))
    warnings = ['KJV word links are unverified for NIV words; no exact NIV Strong\'s mappings were inferred.',
                'This NIV source has no verified Jesus red-letter markup. KJV red letters were not transferred to NIV.',
                'Additional topical and general Bible dictionaries are not imported by this sync.',
                'Personal study records remain attached to their original verse revisions.']
    conn = sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=30)
    try:
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('BEGIN IMMEDIATE')
        changed = False
        for translation in ('NIV', 'KJV'):
            source, content = sources[translation], parsed[translation]
            previous_references = {tuple(row) for row in conn.execute('''SELECT v.book_id,v.chapter,v.verse
                FROM bible_verses v JOIN bible_editions e ON e.id=v.edition_id
                WHERE e.translation=? AND e.is_active=1 AND v.is_active=1''', (translation,))}
            if not previous_references.issubset(content['verses']):
                raise SyncError(translation + ' source is missing previously indexed verses. Existing data was kept; review the source edition before retrying.')
            digest = source['sha256']
            edition_key = translation + ':' + digest
            existing = conn.execute('SELECT id FROM bible_editions WHERE edition_key=? AND status=\'ready\'', (edition_key,)).fetchone()
            if existing:
                edition_id = existing[0]
                active = conn.execute('SELECT is_active FROM bible_editions WHERE id=?', (edition_id,)).fetchone()[0]
                changed = changed or not bool(active)
                conn.execute('UPDATE bible_editions SET is_active=0 WHERE translation=? AND id!=?', (translation, edition_id))
                conn.execute('UPDATE bible_editions SET is_active=1 WHERE id=?', (edition_id,))
                continue
            changed = True
            conn.execute('''INSERT OR IGNORE INTO bible_sources(title,edition,sha256,source_path,attribution,created_at)
                VALUES(?,?,?,?,?,?)''', (source['title'], translation, digest, 'drive:' + source['drive_file_id'], source['title'], now))
            source_id = conn.execute('SELECT id FROM bible_sources WHERE sha256=?', (digest,)).fetchone()[0]
            run_id = conn.execute('''INSERT INTO bible_import_runs(source_id,status,started_at,finished_at,report_json)
                VALUES(?,'validated',?,?,?)''', (source_id, now, now, json.dumps(counts))).lastrowid
            conn.execute('UPDATE bible_editions SET is_active=0 WHERE translation=?', (translation,))
            edition_id = conn.execute('''INSERT INTO bible_editions(source_id,import_run_id,edition_key,translation,title,status,is_active,attribution)
                VALUES(?,?,?,?,?,'ready',1,?)''', (source_id, run_id, edition_key, translation, source['title'], source['title'])).lastrowid
            for reference, verse in sorted(content['verses'].items()):
                verse_id = conn.execute('''INSERT INTO bible_verses(edition_id,book_id,chapter,verse,text,heading,content_hash)
                    VALUES(?,?,?,?,?,?,?)''', (edition_id, *reference, verse['text'], verse['heading'],
                    hashlib.sha256(verse['text'].encode('utf-8')).hexdigest())).lastrowid
                if translation == 'NIV':
                    conn.executemany('''INSERT INTO bible_tokens(verse_id,token_index,start_offset,end_offset,text) VALUES(?,?,?,?,?)''',
                        ((verse_id, index, word.start(), word.end(), word[0]) for index, word in enumerate(re.finditer(r"\w+(?:['’]\w+)*", verse['text']))))
            if translation == 'KJV':
                entries = {}
                for anchor, entry in content['lexicon'].items():
                    entries[anchor] = conn.execute('''INSERT INTO bible_lexicon(source_id,strong_id,language,lemma,pronunciation,definition,source_anchor)
                        VALUES(?,?,?,?,?,?,?)''', (source_id, entry['strong_id'], entry['language'], entry.get('lemma', ''),
                        entry['pronunciation'], entry['definition'], anchor)).lastrowid
                conn.executemany('''INSERT OR IGNORE INTO bible_concordance(lexicon_id,source_id,book_id,chapter,verse,source_word,source_anchor)
                    VALUES(?,?,?,?,?,?,?)''', ((entries[target], source_id, *reference, phrase, locator)
                    for reference, target, phrase, locator in content['links']))
        report = {'status': 'completed', 'message': 'Bible Study sync completed.' if changed else 'Sources are unchanged. Bible Study is already up to date.',
                  'updated_at': now, 'counts': counts, 'warnings': warnings}
        conn.execute("INSERT INTO bible_metadata(key,value) VALUES('sync_status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(report),))
        conn.commit()
        return report
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _official_drive_sources(drive, folder_id=''):
    """Only the official folder is authoritative; no library catalog is read."""
    if not folder_id:
        folders = [item for item in resources.list_drive_folder(drive, resources.PASTOR_RESOURCES_DRIVE_FOLDER_ID)
                   if item.get('mimeType') == resources.GOOGLE_DRIVE_FOLDER_MIME
                   and item.get('name', '').strip().casefold() == 'for scripture study app']
        if len(folders) != 1:
            raise SyncError('Expected one For Scripture Study App folder in Pastor\'s Resources. Check the folder and Drive sharing permissions.')
        folder_id = folders[0]['id']
    if not re.fullmatch(r'[A-Za-z0-9_-]{10,200}', folder_id):
        raise SyncError('The official Bible Study folder ID is invalid.')
    files = resources.list_drive_folder(drive, folder_id)
    selected, ignored = {}, []
    for item in files:
        name = item.get('name', '')
        if not name.lower().endswith(('.epub','.pdf')):
            ignored.append(name)
            continue
        normalized = re.sub(r'[^a-z0-9]+', ' ', name.casefold()).strip()
        thompson=name.lower().endswith('.pdf') and all(term in normalized for term in ('niv','thompson','chain reference'))
        family = ('NIV' if normalized == 'holy bible niv epub' or thompson else
                  'mounce' if normalized.startswith('mounce') and 'expository dictionary' in normalized else
                  'vine' if normalized.startswith('vine') and 'expository dictionary' in normalized else '')
        if not family:
            ignored.append(name)
            continue
        if family in selected:
            if family=='NIV' and selected[family].get('thompson')!=thompson:
                if not thompson:
                    ignored.append(name);continue
                ignored.append(selected[family]['name'])
            else:
                raise SyncError('More than one '+family.title()+' source was found. Keep one current source for each book.')
        fingerprint = {key: item.get(key, '') for key in ('id', 'modifiedTime', 'size', 'md5Checksum', 'sha256Checksum')}
        selected[family] = dict(name=name, title=name.rsplit('.', 1)[0], drive_file_id=item['id'],
                                size=item.get('size', 0), fingerprint=fingerprint,thompson=thompson)
    if not selected:
        raise SyncError('No Thompson NIV PDF, NIV EPUB, Mounce or Vine EPUBs were found directly inside For Scripture Study App.')
    return folder_id, selected, ignored


def _validate_dictionary(parsed, family):
    entries = parsed['entries']
    if len(entries) < (13000 if family == 'mounce' else 3000):
        raise SyncError(f'{family.title()} validation found fewer entries than expected. Existing data was kept.')
    sections = {entry['section'] for entry in entries}
    if family == 'mounce' and not {'Expository dictionary', 'Hebrew-English Dictionary', 'Greek-English Dictionary'}.issubset(sections):
        raise SyncError('Mounce is missing one of its three dictionary sections.')
    languages = {entry['language'] for entry in entries}
    identifiers = [item for entry in entries for item in entry['identifiers']]
    if not any(item[0] == 'strong' for item in identifiers) or (family == 'vine' and not {'Hebrew', 'Greek'}.issubset(languages)):
        raise SyncError(f'{family.title()} language or identifier coverage is incomplete.')
    if len({entry['locator'] for entry in entries}) != len(entries):
        raise SyncError(f'{family.title()} contains duplicate entry locators.')


def _validate_niv_source(content):
    if len(content['verses']) < 30000 or len({key[:2] for key in content['verses']}) != 1189:
        raise SyncError('NIV validation failed: expected all 66 books and 1,189 chapters.')


def _active_source_hash(conn, family):
    if family!='NIV' and conn.execute('PRAGMA user_version').fetchone()[0]<2:
        return ''
    if family == 'NIV':
        row = conn.execute('''SELECT s.sha256 FROM bible_editions e JOIN bible_sources s ON s.id=e.source_id
            JOIN bible_import_runs r ON r.id=e.import_run_id WHERE e.translation='NIV' AND e.is_active=1
            AND e.status='ready' AND r.status='validated' ''').fetchone()
    else:
        row = conn.execute('''SELECT s.sha256 FROM bible_dictionary_versions d JOIN bible_sources s ON s.id=d.source_id
            JOIN bible_import_runs r ON r.id=d.import_run_id WHERE d.family=? AND d.is_active=1 AND r.status='validated' ''', (family,)).fetchone()
    return row[0] if row else ''


def _sync_preserved_state(conn,baseline=None):
    if baseline is None:
        names=[row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            if row[0] in ('bible_verses','bible_tokens','bible_annotations','bible_bookmarks','bible_reader_state','bible_study_saves',
                          'bible_dictionary_entries','bible_dictionary_terms','bible_dictionary_identifiers','bible_dictionary_references',
                          'bible_dictionary_assets','bible_dictionary_entry_assets','bible_lexicon','bible_concordance')
            or row[0].startswith(('bible_original_','bible_niv_alignment'))]
        baseline={name:(conn.execute(f'SELECT COALESCE(MAX(rowid),0) FROM {name}').fetchone()[0],0,'') for name in names}
    return _step_preserved_state(conn,baseline)


def _publish_dictionary_sync(path, selected, parsed, folder_id, warnings, *, check_preservation=False):
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=30)
    try:
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('BEGIN IMMEDIATE')
        preserved=_sync_preserved_state(conn) if check_preservation else None
        changed = False
        for family, content in parsed.items():
            source = selected[family]
            digest = source['sha256']
            if _active_source_hash(conn, family) == digest:
                if family == 'NIV' and content.get('concordance'):
                    edition_id=conn.execute("SELECT id FROM bible_editions WHERE translation='NIV' AND is_active=1").fetchone()[0]
                    _publish_thompson_concordance(conn,edition_id,content['concordance'])
                    changed = True
                    if content.get('studies'):_publish_thompson_studies(conn,edition_id,content['studies'])
                continue
            conn.execute('''INSERT OR IGNORE INTO bible_sources(title,edition,sha256,source_path,attribution,created_at)
                VALUES(?,?,?,?,?,?)''', (source['title'], family, digest, 'drive:' + source['drive_file_id'], source['title'], now))
            source_id = conn.execute('SELECT id FROM bible_sources WHERE sha256=?', (digest,)).fetchone()[0]
            run_id = conn.execute("INSERT INTO bible_import_runs(source_id,status,started_at,finished_at) VALUES(?,'validated',?,?)", (source_id, now, now)).lastrowid
            changed = True
            if family == 'NIV':
                previous = {tuple(row) for row in conn.execute('''SELECT v.book_id,v.chapter,v.verse FROM bible_verses v
                    JOIN bible_editions e ON e.id=v.edition_id WHERE e.translation='NIV' AND e.is_active=1 AND v.is_active=1''')}
                if not previous.issubset(content['verses']):
                    raise SyncError('The NIV source is missing previously indexed verses. Existing data was kept.')
                existing = conn.execute('SELECT id FROM bible_editions WHERE edition_key=?', ('NIV:'+digest,)).fetchone()
                conn.execute("UPDATE bible_editions SET is_active=0 WHERE translation='NIV'")
                if existing:
                    conn.execute('UPDATE bible_editions SET is_active=1 WHERE id=?', (existing[0],))
                    if content.get('concordance'):
                        _publish_thompson_concordance(conn,existing[0],content['concordance'])
                    if content.get('studies'):_publish_thompson_studies(conn,existing[0],content['studies'])
                    continue
                edition_id = conn.execute('''INSERT INTO bible_editions(source_id,import_run_id,edition_key,translation,title,status,is_active,attribution)
                    VALUES(?,?,?,'NIV',?,'ready',1,?)''', (source_id, run_id, 'NIV:'+digest, source['title'], content.get('attribution',source['title']))).lastrowid
                verse_lookup={}
                for reference, verse in sorted(content['verses'].items()):
                    verse_id = conn.execute('''INSERT INTO bible_verses(edition_id,book_id,chapter,verse,text,heading,content_hash)
                        VALUES(?,?,?,?,?,?,?)''', (edition_id, *reference, verse['text'], verse['heading'], hashlib.sha256(verse['text'].encode()).hexdigest())).lastrowid
                    conn.executemany('INSERT INTO bible_tokens(verse_id,token_index,start_offset,end_offset,text) VALUES(?,?,?,?,?)',
                        ((verse_id, i, word.start(), word.end(), word[0]) for i, word in enumerate(re.finditer(r"\w+(?:['’]\w+)*", verse['text']))))
                    verse_lookup[reference]=verse_id
                    conn.executemany('''INSERT INTO bible_red_letter_spans(verse_id,start_offset,end_offset,speaker,evidence_source_id,evidence_locator)
                        VALUES(?,?,?,'Jesus',?,?)''',[(verse_id,start,end,source_id,'PDF page '+str(verse['source_page'])+'; scripture red fill') for start,end in verse.get('red_spans',[])])
                if content.get('thompson'):
                    conn.execute('INSERT INTO bible_edition_details(edition_id,label,source_kind,metadata_json) VALUES(?,?,?,?)',
                        (edition_id,content['edition_label'],'thompson_pdf',json.dumps(dict(edition_year=2011,publication_year=2022))))
                    for key,topic in content['topics'].items():
                        blocks=topic.get('blocks') or [dict(type='paragraph',text=topic['text'],references=topic['references'])]
                        topic_id=conn.execute('''INSERT INTO bible_chain_topics(edition_id,topic_key,title,content_json,source_page)
                            VALUES(?,?,?,?,?)''',(edition_id,key,topic['title'],json.dumps(blocks,ensure_ascii=False),topic['source_page'])).lastrowid
                        linked=set()
                        for ref in topic['references']:
                            for chapter in range(ref['chapter'],ref['end_chapter']+1):
                                for number in range(ref['verse'] if chapter==ref['chapter'] else 1,
                                    (ref['end_verse'] if chapter==ref['end_chapter'] else 176)+1):
                                    found=verse_lookup.get((ref['book_id'],chapter,number))
                                    if found:linked.add(found)
                        conn.executemany('INSERT OR IGNORE INTO bible_chain_verses(verse_id,topic_id) VALUES(?,?)',[(v,topic_id) for v in linked])
                    if content.get('concordance'):
                        _publish_thompson_concordance(conn,edition_id,content['concordance'])
                    if content.get('studies'):_publish_thompson_studies(conn,edition_id,content['studies'])
            else:
                existing = conn.execute('SELECT id FROM bible_dictionary_versions WHERE source_id=? AND family=?', (source_id, family)).fetchone()
                conn.execute('UPDATE bible_dictionary_versions SET is_active=0 WHERE family=?', (family,))
                if existing:
                    conn.execute('UPDATE bible_dictionary_versions SET is_active=1 WHERE id=?', (existing[0],))
                    continue
                version_id = conn.execute('INSERT INTO bible_dictionary_versions(source_id,import_run_id,family,is_active) VALUES(?,?,?,1)', (source_id, run_id, family)).lastrowid
                for asset in content['assets'].values():
                    conn.execute('INSERT OR IGNORE INTO bible_dictionary_assets(sha256,mime_type,data) VALUES(?,?,?)', (asset['sha256'], asset['mime_type'], asset['data']))
                for entry in content['entries']:
                    preview = _text(entry['plain_text'].replace('\ufffc', ''))
                    if len(preview)>360:
                        preview = preview[:360].rsplit(' ', 1)[0]+'…'
                    entry_id = conn.execute('''INSERT INTO bible_dictionary_entries(version_id,locator,headword,section,language,content_json,plain_text,preview)
                        VALUES(?,?,?,?,?,?,?,?)''', (version_id, entry['locator'], entry['headword'], entry['section'], entry['language'],
                        json.dumps(entry['blocks'], ensure_ascii=False), entry['plain_text'], preview)).lastrowid
                    conn.executemany('''INSERT INTO bible_dictionary_terms(entry_id,term,kind,display,complete) VALUES(?,?,?,?,?)
                        ON CONFLICT(entry_id,term,kind) DO NOTHING''',
                                     ((entry_id, *term) for term in entry['terms']))
                    conn.executemany('''INSERT INTO bible_dictionary_identifiers(entry_id,scheme,language,number,qualifier,source_text)
                        VALUES(?,?,?,?,?,?) ON CONFLICT(entry_id,scheme,language,number,qualifier) DO NOTHING''', ((entry_id, *identifier) for identifier in entry['identifiers']))
                    references = [ref for block in entry['blocks'] for ref in block['references']]
                    conn.executemany('''INSERT INTO bible_dictionary_references(entry_id,book_id,chapter,verse,end_chapter,end_verse,source_text)
                        VALUES(?,?,?,?,?,?,?) ON CONFLICT(entry_id,book_id,chapter,verse,end_chapter,end_verse) DO NOTHING''', ((entry_id, ref['book_id'], ref['chapter'], ref['verse'], ref['end_chapter'], ref['end_verse'], ref['source_text']) for ref in references))
                    used = {run['asset'] for block in entry['blocks'] for run in block['runs'] if run['type'] == 'image'}
                    conn.executemany('INSERT INTO bible_dictionary_entry_assets(entry_id,sha256) VALUES(?,?)', ((entry_id, asset) for asset in used))
        for family, source in selected.items():
            if source.get('sha256'):
                conn.execute('INSERT INTO bible_metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                             ('drive_fingerprint:'+family, json.dumps(dict(fingerprint=source['fingerprint'], sha256=source['sha256']))))
        if conn.execute('PRAGMA user_version').fetchone()[0] >= 3:
            _step_link_dictionaries(conn)
        if conn.execute('PRAGMA user_version').fetchone()[0]>=4:
            rebuild_dictionary_senses(conn)
        conn.execute("INSERT INTO bible_metadata(key,value) VALUES('official_drive_folder',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (folder_id,))
        counts = {'niv_verses': conn.execute("SELECT COUNT(*) FROM bible_verses v JOIN bible_editions e ON e.id=v.edition_id WHERE e.translation='NIV' AND e.is_active=1 AND v.is_active=1").fetchone()[0]}
        if conn.execute('PRAGMA user_version').fetchone()[0]>=5:
            counts['thompson_concordance_entries']=conn.execute('''SELECT COUNT(*) FROM bible_thompson_concordance c
                JOIN bible_editions e ON e.id=c.edition_id WHERE e.is_active=1''').fetchone()[0]
        if conn.execute('PRAGMA user_version').fetchone()[0]>=6:
            counts['book_studies']=conn.execute('''SELECT COUNT(*) FROM bible_thompson_studies s
                JOIN bible_editions e ON e.id=s.edition_id WHERE s.kind='book' AND e.is_active=1''').fetchone()[0]
            counts['person_profiles']=conn.execute('''SELECT COUNT(*) FROM bible_person_profiles p
                JOIN bible_editions e ON e.id=p.edition_id WHERE e.is_active=1''').fetchone()[0]
        for family in ('mounce', 'vine'):
            counts[family+'_entries'] = conn.execute('SELECT COUNT(*) FROM bible_dictionary_entries e JOIN bible_dictionary_versions d ON d.id=e.version_id WHERE d.family=? AND d.is_active=1', (family,)).fetchone()[0]
        report = dict(status='completed', message='Bible Study sync completed.' if changed else 'Sources are unchanged. Bible Study is already up to date.',
                      counts=counts, warnings=warnings, updated_at=now)
        conn.execute("INSERT INTO bible_metadata(key,value) VALUES('sync_status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(report),))
        if preserved is not None and _sync_preserved_state(conn,preserved)!=preserved:
            raise SyncError('Validation detected a change to preserved Bible Study records. The update was rolled back.')
        conn.commit()
        return report
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _safe_publish_sources(path,selected,parsed,folder_id,warnings,progress):
    """Validate in a private SQLite copy, then back up before live migration."""
    if path.exists() and not parsed:
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as conn:
            if conn.execute('PRAGMA user_version').fetchone()[0]==SCHEMA_VERSION:
                return _publish_dictionary_sync(path,selected,parsed,folder_id,warnings)
    with tempfile.TemporaryDirectory(prefix='district4_bible_sync_') as directory:
        staged=Path(directory)/'validation.db'
        if path.exists():
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as source, closing(sqlite3.connect(staged)) as target:
                source.backup(target)
        progress('Validating the update in a temporary Bible Study database.')
        initialize_database(staged)
        preview=_publish_dictionary_sync(staged,selected,parsed,folder_id,list(warnings),check_preservation=True)
        with closing(sqlite3.connect(staged)) as check:
            if check.execute('PRAGMA integrity_check').fetchone()[0]!='ok' or check.execute('PRAGMA foreign_key_check').fetchone():
                raise SyncError('Temporary database validation failed. Existing data was kept.')
    backup=None
    if path.exists():
        # Keep recovery files in the same persistent private volume as the DB.
        directory=path.parent/'backups'
        directory.mkdir(parents=True,exist_ok=True)
        backup=directory/('bible_study_before_thompson_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_')+secrets.token_hex(3)+'.db')
        progress('Backing up the existing Bible Study database.')
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as source,closing(sqlite3.connect(backup)) as target:
            source.backup(target)
            if target.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
                raise SyncError('Backup validation failed. Existing data was kept.')
    initialize_database(path)
    report=_publish_dictionary_sync(path,selected,parsed,folder_id,warnings,check_preservation=True)
    if report['counts']!=preview['counts']:
        raise SyncError('Published counts differ from temporary validation. A recovery backup is available.')
    return dict(**report,temporary_validation='passed',backup=str(backup) if backup else None)


def sync_sources(path, folder_id, progress, fetch=None, drive=None, *, translation_folder_id=None):
    """Independent Drive -> Bible Study SQLite sync; no AI/library DB dependency."""
    owned = drive is None
    drive = drive or resources.get_drive_session()
    try:
        progress('Finding Bible Study sources in For Scripture Study App.')
        stored=None
        if path.exists():
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as conn:
                stored = conn.execute("SELECT value FROM bible_metadata WHERE key='official_drive_folder'").fetchone()
        folder_id, selected, ignored = _official_drive_sources(drive, folder_id or (stored[0] if stored else ''))
        parsed, warnings = {}, []
        warnings.append('Dictionary matches are searches, not verified NIV original-language alignments.')
        warnings.append('Mounce images are preserved; image-only spellings are not guessed as Unicode.')
        warnings.append('Legacy KJV/Strong’s records and all personal records are retained.')
        if ignored:
            warnings.append(f'{len(ignored)} unsupported files or subfolders were skipped.')
        for family in ('NIV', 'mounce', 'vine'):
            if family not in selected:
                warnings.append(f'{family.title()} EPUB not found; any previous version was kept.')
                continue
            source = selected[family]
            old={};unchanged=False
            if path.exists():
              with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as conn:
                old = conn.execute('SELECT value FROM bible_metadata WHERE key=?', ('drive_fingerprint:'+family,)).fetchone()
                old = json.loads(old[0]) if old else {}
                unchanged = (source['fingerprint'].get('modifiedTime') and old.get('fingerprint') == source['fingerprint']
                             and old.get('sha256') == _active_source_hash(conn, family))
                if unchanged and source.get('thompson'):
                    unchanged = (conn.execute('PRAGMA user_version').fetchone()[0]>=5
                        and conn.execute('''SELECT 1 FROM bible_thompson_concordance c JOIN bible_editions e ON e.id=c.edition_id
                            WHERE e.is_active=1 LIMIT 1''').fetchone() is not None)
                    if unchanged:
                        unchanged = conn.execute('PRAGMA user_version').fetchone()[0]>=6
                        if unchanged:
                            studies = conn.execute('''SELECT s.content_json FROM bible_thompson_studies s
                                JOIN bible_editions e ON e.id=s.edition_id WHERE s.kind='book' AND e.is_active=1''').fetchall()
                            unchanged = bool(studies) and all((json.loads(row[0]).get('outline') or {}).get('blocks') for row in studies)
            if unchanged:
                source['sha256'] = old['sha256']
                progress(f'{family.title()} is unchanged; keeping its indexed version.')
                continue
            progress('Downloading and indexing ' + family.title() + '.')
            data = fetch(source) if fetch else _download_source(drive, source)
            source['sha256'] = hashlib.sha256(data).hexdigest()
            if family == 'NIV':
                content = parse_thompson_pdf(data,progress) if source.get('thompson') else parse_niv_epub(data)
                _validate_niv_source(content)
            else:
                content = parse_dictionary_epub(data, family)
                _validate_dictionary(content, family)
            parsed[family] = content
        progress('Publishing validated sources. Existing verses and personal records are preserved.')
        result = _safe_publish_sources(path,selected,parsed,folder_id,warnings,progress)
        if translation_folder_id:
            progress('Importing Cebuano and Tagalog into Bible Study only.')
            translations = sync_translation_sources(path,translation_folder_id,progress,drive=drive,fetch=fetch)
            result['counts'].update(translations['counts'])
            result['message'] += ' ' + translations['message']
            _write_sync_status(path,result)
        return result
    finally:
        if owned:
            drive.close()


def _run_sync(app, path, lock):
    def progress(message):
        _write_sync_status(path, {'status': 'running', 'message': message, 'updated_at': datetime.now(timezone.utc).isoformat()})
    try:
        with app.app_context():
            options = ({'translation_folder_id':app.config['BIBLE_STUDY_TRANSLATION_FOLDER_ID']}
                       if app.config.get('BIBLE_STUDY_TRANSLATION_FOLDER_ID') else {})
            sync_sources(path, app.config.get('BIBLE_STUDY_DRIVE_FOLDER_ID', ''), progress, **options)
    except Exception as error:
        logging.getLogger(__name__).exception('Bible Study sync failed')
        message = str(error) if isinstance(error, SyncError) else 'Sync failed while reading the sources or saving data. Check server logs and retry.'
        try:
            _write_sync_status(path, {'status': 'failed', 'message': message,
                                     'updated_at': datetime.now(timezone.utc).isoformat()})
        except Exception:
            logging.getLogger(__name__).exception('Could not persist Bible Study sync status')
    finally:
        _release_sync_lock(lock)


def _database_path(value, static_folder=None, protected_paths=()):
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise ValueError('Bible Study requires an absolute database path.')
    path = Path(value)
    if not path.is_absolute():
        raise ValueError('Bible Study requires an absolute database path.')
    path = path.resolve()
    protected = {Path(item).resolve() for item in protected_paths if item}
    protected.update((BASE_DIR / name).resolve() for name in ('app_v2.db', 'ai_index.db', 'pastor_resource_sync_state.db'))
    if path in protected or path.name.lower() in {'app_v2.db', 'ai_index.db', 'pastor_resource_sync_state.db'}:
        raise ValueError('Bible Study must use its own database.')
    for public_root in (BASE_DIR / 'static', Path(static_folder) if static_folder else None):
        if public_root and path.is_relative_to(public_root.resolve()):
            raise ValueError('Bible Study data must not be stored in public static assets.')
    if path.suffix.lower() != '.db':
        raise ValueError('Use a separate .db file for Bible Study.')
    return path


def get_indexed_niv_passage(reference, database_path, **path_options):
    """Read a complete NIV passage for other website features, without indexing.

    Missing storage, incomplete ranges and unvalidated editions return None.
    This function never creates a database or falls back to another translation.
    """
    if not isinstance(reference, str):
        return None
    match = re.fullmatch(r'(.+?)\s+(\d+):(\d+)(?:\s*[-–]\s*(\d+))?', reference.strip())
    if not match:
        return None
    book_name = _text(match[1]).casefold()
    if book_name == 'psalm':
        book_name = 'psalms'
    book = BOOK_BY_NAME.get(book_name)
    chapter, first = int(match[2]), int(match[3])
    last = int(match[4]) if match[4] else first
    if not book or not (1 <= chapter <= book[2] and 1 <= first <= last <= 176):
        return None
    try:
        path = _database_path(database_path, **path_options)
        if not path.is_file():
            return None
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)) as conn:
            if (conn.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID
                    or conn.execute('PRAGMA user_version').fetchone()[0] not in (1, 2, 3, 4, 5, 6, SCHEMA_VERSION)):
                return None
            rows = conn.execute('''SELECT v.verse,v.text FROM bible_verses v
                JOIN bible_editions e ON e.id=v.edition_id JOIN bible_import_runs r ON r.id=e.import_run_id
                WHERE e.translation='NIV' AND e.is_active=1 AND e.status='ready' AND r.status='validated'
                AND v.is_active=1 AND v.book_id=? AND v.chapter=? AND v.verse BETWEEN ? AND ?
                ORDER BY v.verse''', (book[0], chapter, first, last)).fetchall()
        if [row[0] for row in rows] != list(range(first, last + 1)) or any(not row[1].strip() for row in rows):
            return None
        return ' '.join(row[1] for row in rows)
    except (ValueError, OSError, sqlite3.DatabaseError):
        return None


def _source_documents(conn, rows):
    """Fingerprint original typography and exclude sources absent from the live catalog."""
    columns={row[1] for row in conn.execute('PRAGMA table_info(pij_library_documents)')}
    index_path=Path(conn.execute('PRAGMA database_list').fetchone()[2]).resolve()
    catalog=index_path.parent/'app_v2.db'
    owner=resources._appmod()
    if getattr(owner,'AI_DATABASE',None) and Path(owner.AI_DATABASE).resolve()==index_path:
        catalog=Path(owner.DATABASE).resolve()
    live=None
    if 'drive_file_id' in columns and catalog.is_file():
        with closing(sqlite3.connect(catalog.as_uri()+'?mode=ro',uri=True)) as books:
            if books.execute("SELECT 1 FROM sqlite_master WHERE name='pastor_library_files'").fetchone():
                live={row[0] for row in books.execute('''SELECT f.drive_file_id FROM pastor_library_files f
                    JOIN pastor_library_books b ON b.id=f.book_id WHERE f.is_active=1 AND b.is_active=1
                    AND COALESCE(b.is_hidden,0)=0 AND COALESCE(f.is_duplicate,0)=0''')}
    formats={}
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name='pij_library_source_formats'").fetchone():
        formats={row[0]:list(row[1:]) for row in conn.execute('SELECT document_id,checksum,modified_time,version FROM pij_library_source_formats')}
    drives={row[0]:row[1] for row in conn.execute('SELECT id,drive_file_id FROM pij_library_documents')} if live is not None else {}
    result=[]
    for row in rows:
        document=dict(row)
        if live is not None:
            if drives.get(document['id']) not in live:continue
        document['source_format']=formats.get(document['id'])
        result.append(document)
    return result


_CATALOGUE_BUILD_LOCK=threading.RLock()
_CATALOGUE_PARSER_REVISION=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _persisted_catalogue(kind):
    """Reuse derived private indexes after restarts; source databases stay read-only."""
    def decorate(build):
        @lru_cache(maxsize=1)
        def cached(path, documents):
            key=hashlib.sha256(json.dumps([_CATALOGUE_PARSER_REVISION,str(Path(path).resolve()),documents],sort_keys=True).encode()).hexdigest()
            directory=Path(path).resolve().parent/'bible_study_data'/'catalogue_cache'
            target=directory/(kind+'-'+key+'.json.gz')
            # A second simultaneous first request waits for and reuses the first
            # build, rather than parsing the same multi-volume library again.
            with _CATALOGUE_BUILD_LOCK:
                try:
                    with gzip.open(target,'rt',encoding='utf-8') as stream:stored=json.load(stream)
                    if stored.get('key')==key and isinstance(stored.get('sections'),list):return stored['sections']
                except (OSError,ValueError,EOFError,AttributeError):pass
                sections=build(path,documents)
                temporary=None
                try:
                    directory.mkdir(parents=True,exist_ok=True,mode=0o700)
                    with tempfile.NamedTemporaryFile(dir=directory,prefix=kind+'-',suffix='.tmp',delete=False) as stream:temporary=Path(stream.name)
                    with gzip.open(temporary,'wt',encoding='utf-8',compresslevel=1) as stream:
                        json.dump(dict(key=key,sections=sections),stream,ensure_ascii=False,separators=(',',':'))
                    os.replace(temporary,target)
                except OSError:
                    logging.getLogger(__name__).debug('Derived study cache unavailable; using the in-memory catalogue.')
                finally:
                    try:
                        if temporary:temporary.unlink(missing_ok=True)
                    except OSError:pass
                return sections
        return cached
    return decorate


def _pulpit_citation_emphasis(text, runs):
    match=SCRIPTURE_PATTERN.match(text) or _SINGLE_CHAPTER_REFERENCE.match(text) or re.match(r'^(?:Vers?|Verses)\.?\s*\d+(?:\s*[-–—]\s*\d+)?',text,re.I)
    if not match:return False
    position=0
    for run in runs:
        part=run['text'][:max(0,match.end()-position)]
        if part.strip() and 'strong' not in run.get('marks',()):return False
        position+=len(run['text'])
        if position>=match.end():return True
    return False


def _pulpit_citations(text, context=None):
    # This edition mislabels Ecclesiastes headings "Est.". Only its explicit
    # Ecclesiastes chapter context resolves that abbreviation; Esther elsewhere
    # keeps its normal meaning. Equal-length substitution preserves offsets.
    if context and context[0]==21:text=re.sub(r'^Est(?=\.?\s*\d)','Ecc',text,count=1,flags=re.I)
    return scripture_references(text)


def _indexed_source(conn, document, *, with_styles=True):
    formatted=None
    if document.get('source_format'):
        row=conn.execute('SELECT checksum,modified_time,sections_json FROM pij_library_source_formats WHERE document_id=?',(document['id'],)).fetchone()
        if row and (row[0],row[1])==(document['checksum'],document['modified_time']):
            formatted=json.loads(row[2])
    if formatted is not None:
        fingerprint=(document['checksum'],document['modified_time'],*document['source_format'])
        _store_source_pages(conn.execute('PRAGMA database_list').fetchone()[2],document['id'],fingerprint,formatted)
        rows=[(page,'\n'.join(block['text'] for block in blocks)) for page,blocks in enumerate(formatted,1)]
        styles={'__source_typography__':True,'__source_heading_citations__':set()}
        for blocks in formatted:
            for block in blocks:
                runs=block.get('runs',[])
                if _pulpit_citation_emphasis(block['text'],runs):styles['__source_heading_citations__'].add(block['text'])
                if with_styles:styles.setdefault(block['text'],runs)
                elif runs and all('strong' in run['marks'] for run in runs if run['text'].strip()):
                    styles[block['text']]=True
        return rows,styles
    return conn.execute('SELECT page_start,content FROM pij_library_chunks WHERE document_id=? ORDER BY chunk_number',(document['id'],)).fetchall(),{}


def _source_page_path(path, document_id, fingerprint):
    key=hashlib.sha256(json.dumps([1,str(Path(path).resolve()),document_id,fingerprint]).encode()).hexdigest()
    return Path(path).resolve().parent/'bible_study_data'/'catalogue_cache'/('pages-'+key+'.sqlite')


def _store_source_pages(path, document_id, fingerprint, pages):
    """Disposable indexed pages avoid reparsing a complete volume for one article."""
    target=_source_page_path(path,document_id,fingerprint)
    with _CATALOGUE_BUILD_LOCK:
        if target.is_file():return
        temporary=None
        try:
            target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
            with tempfile.NamedTemporaryFile(dir=target.parent,prefix='pages-',suffix='.tmp',delete=False) as stream:
                temporary=Path(stream.name)
            with closing(sqlite3.connect(temporary)) as cache:
                cache.execute('CREATE TABLE pages(page INTEGER PRIMARY KEY,blocks BLOB NOT NULL)')
                cache.executemany('INSERT INTO pages VALUES (?,?)',((number,gzip.compress(
                    json.dumps(blocks,ensure_ascii=False,separators=(',',':')).encode(),compresslevel=1))
                    for number,blocks in enumerate(pages,1)))
                cache.commit()
            os.replace(temporary,target)
        except (OSError,sqlite3.DatabaseError):
            logging.getLogger(__name__).debug('Derived source page cache unavailable; using the original index.')
        finally:
            try:
                if temporary:temporary.unlink(missing_ok=True)
            except OSError:pass


def _read_source_pages(path, document_id, fingerprint, first, last):
    if not fingerprint or fingerprint[:2]!=fingerprint[2:4]:return None
    target=_source_page_path(path,document_id,fingerprint)
    try:
        with closing(sqlite3.connect(target.as_uri()+'?mode=ro',uri=True,timeout=2)) as cache:
            rows=cache.execute('SELECT page,blocks FROM pages WHERE page BETWEEN ? AND ? ORDER BY page',(first,last)).fetchall()
        if [row[0] for row in rows]!=list(range(first,last+1)):return None
        pages=[json.loads(gzip.decompress(row[1])) for row in rows]
        if not all(isinstance(page,list) and all(isinstance(block,dict) and isinstance(block.get('text'),str) for block in page) for page in pages):return None
        return pages
    except (OSError,sqlite3.DatabaseError,ValueError,EOFError,TypeError,zlib.error):return None


def _study_source_blocks(text, styles=None):
    blocks=[]
    for line in text.splitlines():
        line=line.strip()
        if not line:continue
        block=dict(type='heading' if len(line)<140 and (line.isupper() or re.match(r'^[IVX]+[.)]\s+',line)) else 'paragraph',text=line,references=scripture_references(line))
        runs=(styles or {}).get(line)
        if isinstance(runs,list) and ''.join(run['text'] for run in runs)==line:block['runs']=runs
        if re.match(r'^(?:[IVX]+[.)]|[A-Z][.)]|\d+[.)])\s+',line):
            block.update(role='commentary_outline',level=1 if re.match(r'^[IVX]+[.)]',line) else 2)
        blocks.append(block)
    return blocks


def _apeo_reader_blocks(blocks, title):
    # "Notes" is the printed margin column; the reader already shows the theme.
    result=[];removed_theme=False
    for block in blocks:
        line=block['text'].strip()
        if line.casefold()=='notes' or re.fullmatch(r'\d*\s*SERMON RESOURCE PROJECT\s*\d*',line,re.I):continue
        theme=re.sub(r'^\d+[.)]\s*','',line)
        if not removed_theme and len(result)<3 and _text(theme).casefold()==_text(title).casefold():
            removed_theme=True;continue
        if re.fullmatch(r'\((?:KJV|NKJV|NIV|NASB|ESV|RSV)\)',line):
            block.update(type='paragraph',role='commentary_reference')
        result.append(block)
    for block in result[:5]:
        if scripture_references(block['text']) and len(block['text'])<160:
            block['role']='commentary_reference';break
    return result


def _spurgeon_reader_blocks(blocks, title):
    result=[];removed_title=False;seen_heading=False
    for block in blocks:
        theme=re.sub(r'^\d+[.)]\s*','',block['text'])
        if not removed_title and len(result)<3 and _text(theme).casefold()==_text(title).casefold():
            removed_title=True;continue
        block.pop('marker',None)
        if re.match(r'^[IVXLCDM]+[.)]\s+',block['text']):
            block.update(type='heading',role='sermon_heading',level=1);seen_heading=True
        elif block.get('list_kind') in ('bullet','ordered'):
            block.update(type='paragraph',role='sermon_list_item')
        elif re.match(r'^\d+[.)]\s+',block['text']):
            block.update(type='paragraph',role='sermon_subpoint',level=2)
        elif not seen_heading and len(result)<3 and scripture_references(block['text']):
            block.update(type='paragraph',role='sermon_reference')
        else:block.update(type='paragraph',role='sermon_paragraph')
        result.append(block)
    return result


def _collection_reader_blocks(blocks, title):
    """Use the printed hierarchy of the 52 Sermons books, without renumbering."""
    result=[];removed_title=False
    for block in blocks:
        text=block['text'];theme=re.sub(r'^\d+[.)]\s*','',text)
        if not removed_title and len(result)<3 and _text(theme).casefold()==_text(title).casefold():
            removed_title=True;continue
        block.pop('role',None);block.pop('level',None)
        runs=block.get('runs',[])
        bold=bool(runs) and all('strong' in run.get('marks',[]) for run in runs if run['text'].strip())
        if bold and re.match(r'^(?:[IVXLCDM]+[.)]|\d+[.)])\s+',text):
            block.update(type='heading',role='sermon_heading',level=1)
        elif bold and re.match(r'^[A-Z][.)]\s+',text):
            block.update(type='heading',role='sermon_subpoint',level=2)
        elif bold and len(text)<160:
            block.update(type='heading',role='commentary_section')
        else:block.update(type='paragraph',role='sermon_paragraph')
        refs=block.get('references',[])
        if not refs and re.fullmatch(r'(?:'+_BOOK_PATTERN+r')\.?\s*\d{1,3}',text,re.I):
            printed=_commentary_ranges(text)
            if printed:
                ref=printed[0]
                block['references']=[dict(ref,start=0,end=len(text),book=BOOKS[ref['book_id']-1][0],chapter_only=True)]
                refs=block['references']
        if refs and refs[0]['start']==0 and refs[-1]['end']>=len(text.rstrip(' .')):
            block.update(type='paragraph',role='sermon_reference')
        result.append(block)
    return result


def _wiersbe_reader_blocks(blocks, title):
    for block in blocks:
        block.pop('role',None);block.pop('level',None)
        text=block['text'];runs=block.get('runs',[])
        bold=bool(runs) and all('strong' in run.get('marks',[]) for run in runs if run['text'].strip())
        heading=block.get('source_heading')
        if heading or (bold and len(text)<180):
            block.update(type='heading',role='commentary_heading',level=max(1,min(3,(heading or 2)-1)))
        elif len(text)<180 and re.match(r'^\d+[.)]\s+',text) and text.upper()==text:
            block.update(type='heading',role='commentary_heading',level=1)
        else:block['type']='paragraph'
    return blocks


def _staff_reader_blocks(blocks, title):
    result=[];remaining_title=_text(title).casefold()
    for block in blocks:
        text=block['text']
        if len(result)<3 and re.fullmatch(r'Chapter\s+\d+',text,re.I):continue
        normalized=_text(text).casefold()
        if remaining_title and len(result)<3 and (normalized==remaining_title or remaining_title.startswith(normalized+' ')):
            remaining_title=remaining_title[len(normalized):].lstrip();continue
        block.pop('role',None);block.pop('level',None)
        if len(text)<170 and (re.match(r'^(?:[A-Z]|[IVX]+)[.)]\s+',text) or text in ('Introduction','MY COMMITMENT')):
            block.update(type='heading',role='sermon_heading',level=1)
        elif len(text)<170 and re.match(r'^\d+[.)]\s+',text):
            block.update(type='heading',role='sermon_subpoint',level=2)
        elif len(text)<170 and re.match(r'^[a-z][.)]\s+',text):
            block.update(type='paragraph',role='sermon_subpoint',level=3)
        else:block['type']='paragraph'
        result.append(block)
    return result


def _rich_source_blocks(ai_database, section):
    path=Path(ai_database).resolve()
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=3)) as conn:
        fingerprint=None
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='pij_library_source_formats'").fetchone():
            row=conn.execute('''SELECT d.checksum,d.modified_time,f.checksum,f.modified_time,f.version
                FROM pij_library_documents d LEFT JOIN pij_library_source_formats f ON f.document_id=d.id
                WHERE d.id=?''',(section['document_id'],)).fetchone()
            if row:fingerprint=tuple(row)
    # Callers may add presentation metadata. Keep cached source blocks immutable.
    return copy.deepcopy(_cached_rich_source_blocks(str(path),json.dumps(section,sort_keys=True),fingerprint))


@lru_cache(maxsize=48)
def _cached_rich_source_blocks(ai_database, encoded_section, fingerprint):
    """Read typography only for the opened article, without retaining whole books."""
    section=json.loads(encoded_section)
    styles={};originals={};first=section['source_section'];last=section.get('source_end_section',first)
    wanted={line.strip() for line in section['text'].splitlines()}
    pages=_read_source_pages(ai_database,section['document_id'],fingerprint,first,last)
    if pages is None:
        with closing(sqlite3.connect(Path(ai_database).resolve().as_uri()+'?mode=ro',uri=True,timeout=3)) as conn:
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name='pij_library_source_formats'").fetchone():
                # Fall back to the authoritative formatting when derived pages
                # have not been prepared or their cache is damaged.
                paths=[f'$[{page-1}]' for page in range(first,last+1)]
                placeholders=','.join('?' for _ in paths)
                row=conn.execute(f'''SELECT json_extract(f.sections_json,{placeholders}) FROM pij_library_source_formats f
                    JOIN pij_library_documents d ON d.id=f.document_id
                    WHERE f.document_id=? AND f.checksum=d.checksum AND f.modified_time=d.modified_time''',
                    (*paths,section['document_id'])).fetchone()
                if row and row[0]:
                    pages=json.loads(row[0]);pages=[pages] if len(paths)==1 else pages
    for page in pages or []:
        for block in page or []:
            if block['text'] in wanted:
                styles.setdefault(block['text'],block.get('runs'))
                originals.setdefault(block['text'],[]).append(block)
    blocks=_study_source_blocks(section['text'],styles)
    if section.get('family')=='pulpit' and blocks and section['ranges'][0]['book_id']==21:
        blocks[0]['references']=_pulpit_citations(blocks[0]['text'],(21,section['ranges'][0]['chapter']))
    if section.get('family')=='apeo':
        blocks=_apeo_reader_blocks(blocks,section['title'])
    if section.get('family') in ('spurgeon','collection','wiersbe','staff'):
        for block in blocks:
            matches=originals.get(block['text'])
            if matches:
                original=matches.pop(0)
                for key in ('runs','list_kind','list_level','list_item','list_number','list_style','source_quote','source_heading'):
                    if key in original:block[key]=original[key]
        reader={'spurgeon':_spurgeon_reader_blocks,'collection':_collection_reader_blocks,
                'wiersbe':_wiersbe_reader_blocks,'staff':_staff_reader_blocks}[section['family']]
        blocks=reader(blocks,section['title'])
    return blocks


def _commentary_ranges(text):
    """Parse complete source heading citations, including whole-chapter ranges.

    These ranges are section coverage, not claims that every verse is discussed.
    Only callers that have identified a section heading may use this parser.
    """
    result, book, previous_chapter, previous_verse, separator = [], None, None, None, None
    for part in re.split(r'([;,])\s*', _text(text).strip('() ')):
        if part in (';', ','):
            separator = part
            continue
        part = part.strip()
        whole_book = re.fullmatch(_BOOK_PATTERN, part, re.I)
        if whole_book:
            book = _REF_NAMES[re.sub(r'[.\s]+', '', part).casefold()]
            result.append(dict(book_id=book, chapter=1, verse=1, end_chapter=BOOKS[book-1][1], end_verse=176))
            continue
        match = re.fullmatch(r'(?:(?P<book>' + _BOOK_PATTERN + r')\.?\s*)?'
            r'(?P<first>\d{1,3})(?::(?P<verse>\d{1,3}))?'
            r'(?:\s*[-–—]\s*(?P<last>\d{1,3})(?::(?P<endverse>\d{1,3}))?)?', part, re.I)
        if not match:
            return []
        if match['book']:
            book = _REF_NAMES.get(re.sub(r'[.\s]+', '', match['book']).casefold())
        if not book:
            return []
        chapter, verse = int(match['first']), int(match['verse'] or 1)
        if separator == ',' and not match['book'] and not match['verse'] and previous_verse:
            chapter, verse = previous_chapter, int(match['first'])
            end_chapter, end_verse = chapter, int(match['last'] or match['first'])
        elif BOOKS[book-1][1] == 1 and not match['verse']:
            chapter, verse = 1, int(match['first'])
            end_chapter, end_verse = 1, int(match['last'] or match['first'])
        elif match['verse']:
            end_chapter = int(match['last']) if match['endverse'] else chapter
            end_verse = int(match['endverse'] or match['last'] or match['verse'])
        else:
            end_chapter, end_verse = int(match['last'] or match['first']), 176
        if not (1 <= chapter <= end_chapter <= BOOKS[book-1][1] and 1 <= verse <= 176
                and 1 <= end_verse <= 176 and (chapter, verse) <= (end_chapter, end_verse)):
            return []
        result.append(dict(book_id=book, chapter=chapter, verse=verse, end_chapter=end_chapter, end_verse=end_verse))
        previous_chapter, previous_verse = end_chapter, bool(match['verse']) or separator == ',' and previous_verse
    return result


def _wiersbe_sections(document, rows):
    groups = {}
    for page, content in rows:
        groups.setdefault(page, []).append(content)
    # Long EPUB chapters are split into consecutive index sections. Carry their
    # continuation text forward until the next chapter or back matter begins.
    logical, ends, active = {}, {}, None
    short_title = document['title'].split(' (', 1)[0]
    for page, chunks in groups.items():
        text = _join_index_chunks(chunks)
        text = re.sub(r'^\s*(?:[^\n]+\.x?html|'+re.escape(short_title)+r')\s*\n', '', text, count=1, flags=re.I)
        marker = re.search(r'(?im)^\s*Chapter\s+(?:[A-Za-z-]+|\d+)\s*$', text[:500])
        if re.search(r'(?im)^\s*(?:Notes|About the Author|Bibliography|(?:www\.)?davidccook\.com)\s*$', text[:200]):
            active = None
        elif marker:
            active = page
            logical[page] = text
        elif active is not None and text.strip():
            logical[active] += '\n' + text.strip()
        if active is not None:ends[active]=page
    sections = []
    for page, text in logical.items():
        chapter = re.search(r'(?im)^\s*Chapter\s+(?:[A-Za-z-]+|\d+)\s*$', text)
        if not chapter:
            continue
        if re.search(r'(?im)^\s*Notes\s*$', text[:chapter.start()]):
            continue
        # Chapter markers and a header citation must precede the article body.
        header = text[chapter.end():chapter.end()+1200]
        citation = None
        for match in re.finditer(r'(?m)^\s*\(([^\n]+)\)\s*$', header):
            ranges = _commentary_ranges(match[1])
            if ranges and len(header[:match.start()].split()) <= 45:
                citation = match
                break
        if citation is None:
            continue
        title = _text(header[:citation.start()])
        if not title:
            continue
        body = text[chapter.end()+citation.end():].strip()
        if len(body) < 50:
            continue
        passage = _text(citation[1])
        parallel = re.match(r'^\(See also\s+([^\n]+)\)', body, re.I)
        if parallel:
            parallel_ranges = _commentary_ranges(parallel[1])
            if parallel_ranges:
                ranges = ranges + parallel_ranges
                passage += '; see also ' + _text(parallel[1])
        section = dict(document_id=document['id'], source_title=document['title'].split(' (', 1)[0],
            author='Warren W. Wiersbe', title=title, passage=passage, ranges=ranges,
            source_section=page, source_end_section=ends[page], family='wiersbe', text=body)
        # The Psalms volumes group many psalms into one EPUB chapter. Respect
        # their explicit individual Psalm headings instead of returning a book.
        if all(r['book_id']==19 for r in ranges):
            body = re.split(r'QUESTIONS FOR PERSONAL REFLECTION\s+OR GROUP DISCUSSION', body, maxsplit=1)[0].rstrip()
            psalms = list(re.finditer(r'(?im)^[ \t]*Psalms?\s+(\d{1,3})(?:[ \t]*(?:[–—-]|and|&)[ \t]*(\d{1,3}))?[ \t]*$', body))
        else:
            psalms = []
        if psalms:
            for index, match in enumerate(psalms):
                number, last = int(match[1]), int(match[2] or match[1])
                if not any(r['chapter'] <= number <= last <= r['end_chapter'] for r in ranges):
                    continue
                end = psalms[index+1].start() if index+1 < len(psalms) else len(body)
                passage = 'Psalms '+str(number)+('–'+str(last) if last != number else '')
                sections.append(dict(section, title=passage, passage=passage,
                    ranges=[dict(book_id=19, chapter=number, verse=1, end_chapter=last, end_verse=176)],
                    text=body[match.end():end].strip()))
        else:
            sections.append(section)
    return sections


@_persisted_catalogue('wiersbe')
def _wiersbe_catalogue(path, documents):
    """Cache derived source sections; the shared AI index is always read-only."""
    sections = []
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro', uri=True, timeout=3)) as conn:
        for document_json in documents:
            document = json.loads(document_json)
            rows,_ = _indexed_source(conn,document,with_styles=False)
            if sum(len(row[1]) for row in rows) <= 5_000_000:
                sections.extend(_wiersbe_sections(document, rows))
    return sections


def _format_exalting_outline(blocks):
    """Add presentation metadata without changing indexed wording or offsets."""
    enumeration = re.compile(r'^(?P<label>[IVXLCDM]+|[A-Z]|\d+)[.)]\s+(?P<title>.+)')

    def key(text):
        text = re.sub(r'\s*\([^()]*\d[^()]*\)\s*[.;:]?$', '', text)
        return ''.join(c for c in unicodedata.normalize('NFKC', text).casefold() if c.isalnum())

    def roman(number):
        result = ''
        for value, letters in ((1000,'M'),(900,'CM'),(500,'D'),(400,'CD'),(100,'C'),(90,'XC'),
                               (50,'L'),(40,'XL'),(10,'X'),(9,'IX'),(5,'V'),(4,'IV'),(1,'I')):
            count, number = divmod(number,value)
            result += letters*count
        return result

    points, in_outline, outline_end = {}, False, -1
    for index, block in enumerate(blocks):
        text = block['text']
        match = enumeration.match(text)
        if not in_outline:
            if re.match(r'^Main\s+Idea\s*:',text,re.I):
                block['role'] = 'commentary_main_idea'
            # The article's outline is near the beginning, before its prose.
            if index > 15 or not match or match['label']!='I':
                continue
            in_outline = True
        if match:
            label = match['label']
            level = 1 if re.fullmatch(r'[IVXLCDM]+',label) and (len(label)>1 or label in ('I','V','X','L')) else 3 if label.isdigit() else 2
            block.update(role='commentary_outline',level=level)
            points.setdefault(key(match['title']),dict(level=level,label=label))
            outline_end = index
        elif (len(text)<150 and not re.search(r'[.!?)]\s*$',blocks[index-1]['text'])
              and any(enumeration.match(b['text']) for b in blocks[index+1:index+4])):
            # Retain wrapped PDF outline lines at the preceding point's indent.
            block.update(role='commentary_outline_continuation',level=blocks[index-1].get('level',1))
            outline_end = index
        else:
            break
    main_number = 0
    for index in range(outline_end+1,len(blocks)):
        block = blocks[index]
        text = block['text']
        if len(text)>240 or block.get('role')=='commentary_main_idea':
            continue
        if text.casefold() in ('conclusion','reflect and discuss','reflection and discussion'):
            block.update(type='heading',role='commentary_section')
            continue
        next_block = blocks[index+1] if index+1<len(blocks) else None
        refs = next_block.get('references',[]) if next_block else []
        followed_by_passage = (len(refs)==1 and refs[0]['start']==0 and refs[0]['end']==len(next_block['text']))
        source_marker = enumeration.match(text)
        point = points.get(key(source_marker['title'] if source_marker else text))
        words = re.findall(r'[^\W\d_]+',text)
        looks_like_title = bool(words) and sum(word[0].isupper() for word in words)/len(words)>=.6
        # Standalone passage lines identify main headings even where a printed
        # outline's title differs from the body. Prose is never rewritten to fit.
        if point:
            level, label = point['level'], point['label']
        elif followed_by_passage and looks_like_title:
            level, label = 1, None
        else:
            continue
        if level==1:
            main_number += 1
            label = roman(main_number)
        block.update(type='heading',role='commentary_heading',level=level,marker=None if source_marker else label+'.')
        if followed_by_passage:
            next_block['role'] = 'commentary_reference'
    return blocks


def _commentary_entries(catalogue, book_id, chapter, verse, *, format_outline=False):
    matches = []
    for section in catalogue:
        for span in section['ranges']:
            if (span['book_id'] == book_id and (span['chapter'], span['verse']) <= (chapter, verse)
                    <= (span['end_chapter'], span['end_verse'])):
                width = (span['end_chapter']-span['chapter'])*200+span['end_verse']-span['verse']
                matches.append((width, section))
                break
    entries = []
    for _, section in sorted(matches, key=lambda item: item[0])[:4]:
        blocks = section.get('blocks') or _study_source_blocks(section['text'])
        if not section.get('blocks'):
            for block in blocks:
                block.pop('role',None);block.pop('level',None)
        if format_outline:
            _format_exalting_outline(blocks)
        entries.append({key: section[key] for key in ('document_id','source_title','author','title','passage','source_section')}
                       | dict(blocks=blocks))
    return dict(available=bool(catalogue), entries=entries)


def wiersbe_commentary(ai_database, book_id, chapter, verse):
    path = Path(ai_database).resolve()
    if not path.is_file():
        return dict(available=False, entries=[])
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=3)) as conn:
        conn.row_factory = sqlite3.Row
        documents = conn.execute('''SELECT id,title,checksum,modified_time,indexed_at,chunk_count
            FROM pij_library_documents WHERE source_type='public_ebook' AND searchable=1
            AND lower(author) LIKE '%wiersbe%' AND lower(title) LIKE 'be %' AND lower(format)='epub'
            ORDER BY id''').fetchall()
        documents=_source_documents(conn,documents)
    # Prefer EPUB's paragraph-preserving extraction over duplicate PDF copies.
    unique = {}
    for row in documents:
        unique.setdefault(row['title'].split(' (', 1)[0].casefold(), json.dumps(dict(row), sort_keys=True))
    catalogue = _wiersbe_catalogue(str(path), tuple(unique.values()))
    result=_commentary_entries(catalogue, book_id, chapter, verse)
    for entry in result['entries']:
        section=next(item for item in catalogue if item['document_id']==entry['document_id']
                     and item['source_section']==entry['source_section'] and item['title']==entry['title'])
        entry['blocks']=_rich_source_blocks(path,section)
    return result


def _exalting_ranges(text):
    # Some source headings join disjoint passages with AND rather than a
    # semicolon. This normalization applies only to the identified heading.
    text = re.sub(r'\s+(?:and|&)\s+(?=\d)', '; ', _text(text), flags=re.I)
    return _commentary_ranges(text)


def _exalting_sections(document, rows):
    """Locate the series' passage heading immediately before its Main Idea.

    EPUB sections can contain several articles; PDF articles span many pages.
    Never use incidental Scripture citations in the article as its coverage.
    """
    groups = {}
    for page, content in rows:
        groups.setdefault(page, []).append(content)
    lines = []
    for page, chunks in groups.items():
        group = [line.strip() for line in _join_index_chunks(chunks).splitlines() if line.strip()]
        if group and (re.match(r'^(?:Exalting Jesus|Christ-Centered Exposition|Christ-Centered-[\w-]+\d$|Unknown$)', group[0], re.I)
                      or re.fullmatch(r'[^\n]+\.x?html', group[0], re.I)):
            group.pop(0)
        if document.get('format') == 'pdf' and group and group[0].isdigit():
            group.pop(0)
        lines.extend(dict(text=line, page=page, in_page=index) for index, line in enumerate(group))
    copyright_match = re.search(r'©\s*(?:Copyright\s*)?\d{4}\s+by\s+([^\n]+)',
                                '\n'.join(line['text'] for line in lines[:2000]), re.I)
    author = copyright_match[1].strip() if copyright_match else document.get('author','')
    if not author or author.casefold() in ('commentaries','unknown'):
        author = 'Christ-Centered Exposition contributors'
    starts = []
    for index, line in enumerate(lines):
        if index == 0 or not re.match(r'^Main\s+Idea\s*:', line['text'], re.I):
            continue
        citation_index = index-1
        # Some volumes place an epigraph between the main passage heading and
        # Main Idea. Its closing citation is not the article's passage range.
        scan_start = max(0,index-4)
        for offset in range(index-1,scan_start-1,-1):
            if re.match(r'^Main\s+Idea\s*:',lines[offset]['text'],re.I):
                scan_start=offset+1
                break
        for offset in range(scan_start,index):
            candidate = lines[offset]
            if (candidate['page']==line['page'] and candidate['in_page']<=7
                    and not re.fullmatch(_BOOK_PATTERN,candidate['text'],re.I)
                    and _exalting_ranges(candidate['text'])):
                citation_index=offset
                break
        previous = lines[citation_index]['text']
        inline = re.fullmatch(r'(.+?)\s*\(([^()]+)\)', previous)
        ranges = _exalting_ranges(inline[2] if inline else previous)
        if not ranges:
            continue
        if inline:
            title, title_start = inline[1].strip(), citation_index
        else:
            title_start = citation_index-1
            if title_start < 0:
                continue
            # PDF headings wrap over several short lines at the top of a page.
            # For headings later in a shared EPUB section, take only the title
            # line directly before the passage rather than preceding prose.
            if lines[citation_index]['in_page'] <= 7:
                while (title_start > 0 and citation_index-title_start < 4
                       and lines[title_start-1]['page'] == line['page']
                       and len(lines[title_start-1]['text']) > 1
                       and not lines[title_start-1]['text'].isdigit()
                       and not _commentary_ranges(lines[title_start-1]['text'])):
                    title_start -= 1
            title = ' '.join(item['text'] for item in lines[title_start:citation_index])
        if not 2 <= len(title) <= 350:
            continue
        starts.append(dict(start=title_start, body=citation_index+1, title=title, passage=inline[2] if inline else previous,
                           ranges=ranges, page=lines[citation_index]['page']))
    sections = []
    for index, start in enumerate(starts):
        end = starts[index+1]['start'] if index+1 < len(starts) else len(lines)
        for offset in range(start['body']+1,end):
            if re.fullmatch(r'(?:Works Cited|Scripture Index|Subject Index|Bibliography|About the Authors?)',
                            lines[offset]['text'],re.I):
                end=offset
                break
        text = '\n'.join(line['text'] for line in lines[start['body']:end])
        if len(text) < 50:
            continue
        sections.append(dict(document_id=document['id'], source_title=document['title'].split(' (',1)[0],
            author=author, title=start['title'], passage=start['passage'], ranges=start['ranges'],
            source_section=start['page'], text=text))
    return sections


@_persisted_catalogue('exalting')
def _exalting_catalogue(path, documents):
    sections = []
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro', uri=True, timeout=3)) as conn:
        for document_json in documents:
            document = json.loads(document_json)
            rows = conn.execute('SELECT page_start,content FROM pij_library_chunks WHERE document_id=? ORDER BY chunk_number',
                                (document['id'],)).fetchall()
            if sum(len(row[1]) for row in rows) <= 5_000_000:
                sections.extend(_exalting_sections(document, rows))
    return sections


def exalting_commentary(ai_database, book_id, chapter, verse):
    path = Path(ai_database).resolve()
    if not path.is_file():
        return dict(available=False, entries=[])
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=3)) as conn:
        conn.row_factory = sqlite3.Row
        documents = conn.execute('''SELECT id,title,author,format,checksum,modified_time,indexed_at,chunk_count
            FROM pij_library_documents WHERE source_type='public_ebook' AND searchable=1
            AND lower(title) LIKE 'exalting jesus%' AND lower(format) IN ('epub','pdf')
            ORDER BY CASE lower(format) WHEN 'epub' THEN 0 ELSE 1 END,id''').fetchall()
        documents=_source_documents(conn,documents)
    unique = {}
    for row in documents:
        unique.setdefault(row['title'].split(' (',1)[0].casefold(), json.dumps(dict(row),sort_keys=True))
    return _commentary_entries(_exalting_catalogue(str(path),tuple(unique.values())),book_id,chapter,verse,format_outline=True)


def _moody_sections(document, rows, styles=None, *, with_blocks=True):
    pages={}
    for page,content in rows:pages.setdefault(page,[]).append(content)
    lines=[]
    for page,chunks in pages.items():
        lines.extend((page,line.strip()) for line in _join_index_chunks(chunks).splitlines() if line.strip())
    starts=[]
    for index,(page,line) in enumerate(lines):
        book=BOOK_BY_NAME.get(line.casefold())
        following=[value for _,value in lines[index+1:index+8]]
        if (book and line.isupper() and following and following[0].isupper()
                and any(value.startswith('OUTLINE') or value=='Introduction' for value in following)):
            starts.append((index,book))
    sections=[]
    for ordinal,(start,book) in enumerate(starts):
        citation=re.compile(r'^(\d{1,3}'+(r'(?::\d{1,3})?' if book[2]==1 else r':\d{1,3}')+r'(?:\s*[-–—]\s*(?:\d{1,3}:)?\d{1,3})?)\.\s+')
        end=starts[ordinal+1][0] if ordinal+1<len(starts) else len(lines)
        # Outline references are parenthesized; only paragraph-leading exposition counts.
        entries=[]
        commentary_start=next((offset for offset in range(start+1,end)
                               if lines[offset][1].startswith('COMMENTARY ON ')),start+1)
        for index in range(commentary_start,end):
            match=citation.match(lines[index][1])
            if match:
                ranges=_commentary_ranges(book[1]+' '+match[1])
                if ranges:entries.append((index,match[1],ranges))
        for entry_index,(index,passage,ranges) in enumerate(entries):
            stop=entries[entry_index+1][0] if entry_index+1<len(entries) else end
            for offset in range(index+1,stop):
                if re.fullmatch(r'(?:BIBLIOGRAPHY|SCRIPTURE INDEX|SUBJECT INDEX|BIBLIOGRAPHY FOR .+)',lines[offset][1],re.I):
                    stop=offset;break
            text='\n'.join(line for _,line in lines[index:stop])
            if len(text)<40:continue
            section=dict(document_id=document['id'],source_title=document['title'],author=lines[start+1][1],
                title=book[1]+' '+passage,passage=book[1]+' '+passage,ranges=ranges,source_section=lines[index][0],
                source_end_section=lines[stop-1][0],text=text)
            if with_blocks:section['blocks']=_study_source_blocks(text,styles)
            sections.append(section)
    return sections


@_persisted_catalogue('moody')
def _moody_catalogue(path, documents):
    sections=[]
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True,timeout=3)) as conn:
        for encoded in documents:
            document=json.loads(encoded);rows,styles=_indexed_source(conn,document,with_styles=False)
            sections.extend(_moody_sections(document,rows,with_blocks=False))
    return sections


def moody_commentary(ai_database, book_id, chapter, verse):
    path=Path(ai_database).resolve()
    if not path.is_file():return dict(available=False,entries=[])
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=3)) as conn:
        conn.row_factory=sqlite3.Row
        rows=conn.execute('''SELECT id,title,author,format,checksum,modified_time,indexed_at,chunk_count
            FROM pij_library_documents WHERE source_type='public_ebook' AND searchable=1
            AND lower(title) LIKE '%moody bible commentary%' ORDER BY CASE lower(format) WHEN 'epub' THEN 0 ELSE 1 END,id''').fetchall()
        documents=_source_documents(conn,rows)
    # One preferred edition, with complete typography included in its fingerprint.
    encoded=tuple(json.dumps(doc,sort_keys=True) for doc in documents[:1])
    catalogue=_moody_catalogue(str(path),encoded)
    result=_commentary_entries(catalogue,book_id,chapter,verse)
    for entry in result['entries']:
        section=next(item for item in catalogue if item['title']==entry['title'] and item['source_section']==entry['source_section'])
        entry['blocks']=_rich_source_blocks(path,section)
    return result


def _pulpit_exposition_sections(document, rows, styles=None):
    """Bound exposition by its printed verse headings and section markers."""
    pages={}
    for page,content in rows:pages.setdefault(page,[]).append(content)
    lines=[(page,line.strip()) for page,chunks in pages.items()
           for line in _join_index_chunks(chunks).splitlines() if line.strip()]
    sections=[];active=False;context=[];start=None;coverage=[]
    def finish(end):
        if start is None or not coverage:return
        text='\n'.join(line for _,line in lines[start:end])
        if len(text)<30:return
        passage='; '.join(f"{BOOKS[r['book_id']-1][0]} {r['chapter']}:{r['verse']}"+
            (f"–{r['end_chapter']}:{r['end_verse']}" if (r['chapter'],r['verse'])!=(r['end_chapter'],r['end_verse']) else '') for r in coverage)
        sections.append(dict(document_id=document['id'],source_title=document['title'],author=document.get('author',''),
            title='Exposition · '+passage,passage=passage,ranges=coverage,source_section=lines[start][0],
            source_end_section=lines[end-1][0],text=text,family='pulpit',section_kind='exposition'))
    for index,(_,line) in enumerate(lines):
        ctx=(context[0]['book_id'],context[0]['chapter']) if context else None
        refs=_pulpit_citations(line,ctx) if (SCRIPTURE_PATTERN.match(line) or _SINGLE_CHAPTER_REFERENCE.match(line)) else []
        exact=bool(refs and refs[0]['start']==0 and refs[-1]['end']>=len(line.rstrip(' .')))
        if exact and index+1<len(lines) and lines[index+1][1].upper()=='EXPOSITION':
            finish(index);active=False;start=None
            context=[{key:ref[key] for key in ('book_id','chapter','verse','end_chapter','end_verse')} for ref in refs]
            continue
        if re.match(r'^EXPOSITION\b',line,re.I):
            finish(index);active=True;start=index+1;coverage=context;continue
        if re.match(r'^HOMILETICS\b|^HOMILIES\b|^CHAPTER\s+\d+\b',line,re.I):
            finish(index);active=False;start=None;continue
        if not active:continue
        relative=re.fullmatch(r'(?:Vers?|Verses)\.?\s*(\d+(?:\s*[-–—]\s*\d+)?)\.?',line,re.I)
        if relative and ctx:
            refs=scripture_references(f'{BOOKS[ctx[0]-1][0]} {ctx[1]}:{relative[1]}');exact=bool(refs)
        if not exact:continue
        # A quoted citation on a separate body line is not a new article.
        runs=(styles or {}).get(line)
        emphasized=line in (styles or {}).get('__source_heading_citations__',()) or runs is True or (
            isinstance(runs,list) and _pulpit_citation_emphasis(line,runs))
        if (styles or {}).get('__source_typography__') and not emphasized:continue
        finish(index);start=index
        coverage=[{key:ref[key] for key in ('book_id','chapter','verse','end_chapter','end_verse')} for ref in refs]
    finish(len(lines))
    return sections


@_persisted_catalogue('pulpit')
def _pulpit_commentary_catalogue(path, documents):
    sections=[]
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True,timeout=5)) as conn:
        for encoded in documents:
            document=json.loads(encoded);rows,styles=_indexed_source(conn,document,with_styles=False)
            sections.extend(_pulpit_exposition_sections(document,rows,styles))
            for section in _pulpit_sections(document,rows,styles,with_blocks=False):
                if section.get('section_kind')=='homiletics':
                    section=dict(section,passage='; '.join(
                        f"{BOOKS[r['book_id']-1][0]} {r['chapter']}:{r['verse']}–{r['end_chapter']}:{r['end_verse']}"
                        for r in section['ranges']))
                    sections.append(section)
    return sections


def pulpit_commentary(ai_database, book_id, chapter, verse, section_kind='exposition'):
    path=Path(ai_database).resolve()
    if not path.is_file():return dict(available=False,entries=[])
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as conn:
        conn.row_factory=sqlite3.Row
        rows=conn.execute('''SELECT id,title,author,format,checksum,modified_time,indexed_at,chunk_count
            FROM pij_library_documents WHERE source_type='public_ebook' AND searchable=1
            AND lower(title) LIKE '%pulpit commentary complete volume%'
            ORDER BY CASE lower(format) WHEN 'epub' THEN 0 ELSE 1 END,id''').fetchall()
        documents=_source_documents(conn,rows)
    unique={}
    for doc in documents:
        volume=re.search(r'\bvolume\s+([1-9])\b',doc['title'],re.I)
        if volume:unique.setdefault(volume[1],json.dumps(doc,sort_keys=True))
    catalogue=_pulpit_commentary_catalogue(str(path),tuple(unique.values()))
    selected=[section for section in catalogue if section['section_kind']==section_kind]
    result=_commentary_entries(selected,book_id,chapter,verse)
    result['available']=bool(catalogue)
    greek=re.compile(r'[\u0370-\u03ff\u1f00-\u1fff][\u0370-\u03ff\u1f00-\u1fff\u0300-\u036f]*(?:[ \t]+[\u0370-\u03ff\u1f00-\u1fff][\u0370-\u03ff\u1f00-\u1fff\u0300-\u036f]*)*')
    for entry in result['entries']:
        section=next(s for s in selected if s['document_id']==entry['document_id'] and
            s['source_section']==entry['source_section'] and s['title']==entry['title'])
        entry['blocks']=_rich_source_blocks(path,section)
        for block in entry['blocks']:
            block['greek_links']=[dict(start=m.start(),end=m.end()) for m in greek.finditer(block['text'])
                if any(unicodedata.category(char).startswith('L') for char in m[0])]
            if block['greek_links']:
                block['greek_source']=dict(source_title=entry['source_title'],passage=entry['passage'],
                    source_section=entry['source_section'],book_id=book_id,chapter=chapter,verse=verse)
    return result


def pulpit_greek_dictionary_match(conn, spelling, book_id, chapter, verse):
    """Exact original spelling in this verse only; never infer an NIV alignment."""
    if conn.execute('PRAGMA user_version').fetchone()[0]<3:return None
    def key(value):
        return ''.join(c for c in unicodedata.normalize('NFD',value).casefold()
                       if not c.isspace() and unicodedata.category(c)!='Mn')
    rows=conn.execute('''SELECT t.spelling,l.lemma,l.standard_strong FROM bible_original_tokens t
        JOIN bible_original_versions v ON v.id=t.version_id JOIN bible_import_runs r ON r.id=v.import_run_id
        JOIN bible_original_token_identifiers i ON i.token_id=t.id JOIN bible_original_lexemes l ON l.id=i.lexeme_id
        WHERE t.book_id=? AND t.chapter=? AND t.verse=? AND t.language='Greek' AND t.token_kind='word'
        AND v.is_active=1 AND r.status='validated' ''',(book_id,chapter,verse)).fetchall()
    matches={(row['lemma'],row['standard_strong']) for row in rows if key(row['spelling'])==key(spelling) and row['standard_strong']}
    if len(matches)!=1:return None
    lemma,strong=matches.pop()
    found=search_dictionaries(conn,strong)
    if not any(group['entries'] for group in found['sources']):return None
    return dict(lemma=lemma,query=strong)


def _pulpit_sections(document, rows, styles=None, *, with_blocks=True):
    pages={}
    for page,content in rows:pages.setdefault(page,[]).append(content)
    lines=[]
    for page,chunks in pages.items():
        lines.extend((page,line.strip()) for line in _join_index_chunks(chunks).splitlines() if line.strip())
    entries=[];active=False;contributor='';context=None;consumed_until=-1;section_kind=''
    strict=bool((styles or {}).get('__source_typography__'))
    def bold(value):
        runs=(styles or {}).get(value)
        return runs is True or (isinstance(runs,list) and bool(runs) and
            all('strong' in run['marks'] for run in runs if run['text'].strip()))
    def citation_bold(value):
        return (value in (styles or {}).get('__source_heading_citations__',()) or bold(value)
                or (isinstance((styles or {}).get(value),list) and _pulpit_citation_emphasis(value,styles[value])))
    def prefix(value):
        # Only adjacent citations at the START of the printed heading determine
        # coverage. Scripture mentioned later in a title is part of that title.
        if SCRIPTURE_PATTERN.match(value) or _SINGLE_CHAPTER_REFERENCE.match(value):
            references=_pulpit_citations(value,context)
            if not references or references[0]['start']!=0:return None
            refs=[references[0]]
            for following in references[1:]:
                if not re.fullmatch(r'[\s,;]*',value[refs[-1]['end']:following['start']]):break
                refs.append(following)
            return refs,value[refs[-1]['end']:]
        relative=re.match(r'^(?:Vers?|Verses)\.?\s*(\d{1,3}(?:\s*[-–—]\s*\d{1,3})?)',value,re.I)
        if relative and context:
            refs=scripture_references(f'{BOOKS[context[0]-1][0]} {context[1]}:{relative[1]}')
            if refs:return refs,value[relative.end():]
        return None
    def section_marker(value):
        return bool(re.match(r'^HOMILETICS\b|^HOMILIES\b|^EXPOSITION\b|^CHAPTER\s+\d+\b',value,re.I))
    def outline_point(value):
        return bool(re.match(r'^\(?\d+[.)]|^[IVXLCDM]+[.)]|^[A-Z][.)]\s+',value))
    for index,(page,line) in enumerate(lines):
        if index<=consumed_until:continue
        if re.match(r'^HOMILETICS\b|^HOMILIES\b',line,re.I):
            active=True;contributor=line if line.upper().startswith('HOMILIES BY') else ''
            section_kind='homiletics' if line.upper().startswith('HOMILETICS') else 'homilies'
            entries.append(dict(start=index,boundary=True));continue
        if re.match(r'^EXPOSITION\b|^CHAPTER\s+\d+\b',line,re.I):
            active=False;entries.append(dict(start=index,boundary=True));continue
        if len(line)>700:continue
        parsed=prefix(line)
        if not parsed:continue
        refs,tail=parsed
        # A book/chapter heading before EXPOSITION closes the preceding homily
        # and supplies unambiguous context for printed "Ver." headings.
        if not tail.strip(' .') and index+1<len(lines) and lines[index+1][1].upper()=='EXPOSITION':
            context=(refs[0]['book_id'],refs[0]['chapter']);active=False
            entries.append(dict(start=index,boundary=True));continue
        if not active:continue
        last=index
        # A long passage list can wrap before its title begins.
        while tail.rstrip().endswith((',', ';')) and last+1<len(lines) and last-index<3:
            following=prefix(lines[last+1][1])
            if not following or (strict and not citation_bold(lines[last+1][1])):break
            more,tail=following;refs.extend(more);last+=1
        standalone=re.fullmatch(r'[\s.,;]*(?:[—–-]\s*)?(?:Sect(?:ion)?\.?\s*\d+[.:]?)?',tail,re.I)
        inline=re.fullmatch(r'[\s.]*[—–-][\s—–-]*(\S.*)',tail)
        if standalone:
            title_index=last+1
            if title_index>=len(lines):continue
            theme=lines[title_index][1]
            if prefix(theme) or section_marker(theme) or outline_point(theme):continue
            if strict and not (bold(theme) or all(citation_bold(lines[i][1]) for i in range(index,last+1))):continue
            last=title_index
        elif inline:
            if strict and not all(citation_bold(lines[i][1]) for i in range(index,last+1)):continue
            theme=inline[1]
        else:continue
        if outline_point(theme):continue
        # PDF headings may span several bold lines, even over a page boundary.
        while last+1<len(lines) and last-index<7:
            continuation=lines[last+1][1]
            if section_marker(continuation) or outline_point(continuation) or prefix(continuation):break
            if continuation.startswith(('"','“',"'",'‘')):break
            if re.search(r'[.!?]["”’\']?$',theme) and not re.search(r'\b(?:St|Dr|Rev)\.$',theme):break
            if not bold(continuation):
                if document.get('format','').casefold()!='pdf':break
                if not (continuation[:1].islower() or re.search(r'\b(?:a|an|the|of|in|and|to|with|by|for|from|on|our|his|their|its|that|is)\s*$',theme,re.I)):break
            if len(theme)+len(continuation)>600:break
            theme+=' '+continuation;last+=1
        if not 2<=len(theme)<=600:continue
        ranges=[{key:ref[key] for key in ('book_id','chapter','verse','end_chapter','end_verse')} for ref in refs]
        entries.append(dict(start=index,title=theme,ranges=ranges,page=page,contributor=contributor,section_kind=section_kind,
            source_chapter=dict(book_id=context[0],chapter=context[1]) if context else None))
        consumed_until=last
    sections=[]
    for number,entry in enumerate(entries):
        if entry.get('boundary'):continue
        end=entries[number+1]['start'] if number+1<len(entries) else len(lines)
        text='\n'.join(line for _,line in lines[entry['start']:end])
        if len(text)<100:continue
        identifier=hashlib.sha256(json.dumps([document['id'],entry['start'],text],ensure_ascii=False).encode()).hexdigest()[:32]
        section=dict(id=identifier,document_id=document['id'],title=entry['title'],source_title=document['title'],
            author=entry['contributor'] or document['author'],source_section=entry['page'],purpose='Homiletics / homily',family='pulpit',
            source_end_section=lines[end-1][0],ranges=entry['ranges'],source_chapter=entry['source_chapter'],text=text,publisher='',section_kind=entry['section_kind'])
        if with_blocks:section['blocks']=_study_source_blocks(text,styles)
        if with_blocks and ranges[0]['book_id']==21:
            section['blocks'][0]['references']=_pulpit_citations(section['blocks'][0]['text'],(21,ranges[0]['chapter']))
        sections.append(section)
    return sections


def _sermon_family(title):
    title = re.sub(r'[^a-z0-9]+', ' ', title.casefold()).strip()
    if 'sermon outline bible commentary' in title:
        return 'nicoll'
    if 'spurgeon' in title and 'sermon' in title and 'outline' in title:
        return 'spurgeon'
    if re.search(r'pulpit commentary complete volume [1-9]\b',title):
        return 'pulpit'
    if 'shepherds staff' in title or 'shepherd s staff' in title:
        return 'staff'
    if ('sermon resources vol' in title or title.startswith(('sermons on ', 'sermons from ', 'funeral sermons'))
            or title == 'joy'):
        return 'apeo'
    if ('sermon outlines' in title or re.match(r'52 (?:funeral )?sermons', title)):
        return 'collection'
    return None


def _sermon_primary_ranges(text):
    """Opening preaching text only: later illustrations/citations are not coverage."""
    references = scripture_references(text)
    if not references:
        return []
    first = references[0]
    # Adjacent printed texts can jointly identify a sermon; stop at prose.
    ranges = [first]
    for following in references[1:]:
        between = text[ranges[-1]['end']:following['start']]
        if len(between) > 45 or re.search(r'[A-Za-z]{3,}', between):
            break
        ranges.append(following)
    return [{key: item[key] for key in ('book_id', 'chapter', 'verse', 'end_chapter', 'end_verse')}
            for item in ranges]


def _staff_sections(document, rows, styles=None, *, with_blocks=True):
    """Keep printed teaching chapters intact; internal points are not sermons."""
    pages={}
    for page,content in rows:pages.setdefault(page,[]).append(content)
    locations=[];pieces=[];offset=0
    for page,chunks in pages.items():
        text=_join_index_chunks(chunks).strip()+'\n'
        locations.append((offset,page));pieces.append(text);offset+=len(text)
    text=''.join(pieces)
    starts=list(re.finditer(r'(?im)^[ \t]*Chapter[ \t]+\d+[ \t]*\n[ \t]*([^\n]{2,140})',text))
    boundaries=list(re.finditer(r'(?m)^[ \t]*(?:TOPICAL CONCORDANCE|SECTION[ \t]+[A-F](?:\d+|l))[ \t]*$',text))
    result=[]
    for index,match in enumerate(starts):
        end=starts[index+1].start() if index+1<len(starts) else len(text)
        boundary=next((b for b in boundaries if match.end()<b.start()<end),None)
        if boundary:end=boundary.start()
        title=_text(match[1]);header_end=match.end();body=text[match.start():end].strip()
        for continuation in text[header_end:end].splitlines():
            continuation=continuation.strip()
            if not continuation:continue
            words=continuation.split()
            if ((styles or {}).get(continuation) is not True or len(continuation)>100
                    or continuation in ('Introduction','MY COMMITMENT') or scripture_references(continuation)
                    or re.match(r'^(?:[A-Za-z]|\d+)[.)]\s+',continuation)
                    or not words or sum(word[0].isupper() for word in words)/len(words)<.7):break
            title+=' '+continuation;header_end=text.find(continuation,header_end)+len(continuation)
        # Only opening and main outline introductions establish the teaching's
        # coverage. Incidental later quotations do not generate recommendations.
        opening=text[header_end:end]
        main=list(re.finditer(r'(?m)^[ \t]*[A-Z][.)][ \t]+[^\n]+\n',opening))
        candidates=[opening[:min(main[0].start(),2500)] if main else opening[:2500]]
        for n,point in enumerate(main):
            stop=main[n+1].start() if n+1<len(main) else len(opening)
            part=opening[point.end():min(stop,point.end()+1800)]
            subpoint=re.search(r'(?m)^[ \t]*(?:\d+|[a-z])[.)]\s+',part)
            candidates.append(part[:subpoint.start()] if subpoint and subpoint.start()>0 else part[:800])
        # Some lessons print their foundational Scripture immediately beneath
        # a numbered teaching point. Accept that opening quotation, rather than
        # searching the entire point's later illustrations for references.
        numbered=list(re.finditer(r'(?m)^[ \t]*\d+[.)][ \t]+[^\n]+\n',opening))
        for n,point in enumerate(numbered):
            stop=numbered[n+1].start() if n+1<len(numbered) else len(opening)
            part=opening[point.end():min(stop,point.end()+1400)].lstrip()
            first_line=part.split('\n',1)[0]
            refs=scripture_references(first_line)
            if part.startswith(('"','“','‘')) or (refs and refs[0]['start']==0):candidates.append(part)
        ranges=[]
        for candidate in candidates:
            for span in _sermon_primary_ranges(candidate):
                if span not in ranges:ranges.append(span)
        if not ranges or len(body)<100:continue
        first=next(page for offset,page in reversed(locations) if offset<=match.start())
        last=next(page for offset,page in reversed(locations) if offset<end)
        section_id=hashlib.sha256(json.dumps([document['id'],match.start(),body],ensure_ascii=False).encode()).hexdigest()[:32]
        preceding=next((b for b in reversed(boundaries) if b.end()<=match.start()),None)
        credit=None
        if preceding:
            header=text[preceding.end():min(preceding.end()+2000,match.start())]
            # Credits belong to the section heading, before its first chapter.
            header=re.split(r'(?im)^\s*Chapter\s+\d+\s*$',header,maxsplit=1)[0]
            credit=re.search(r'(?m)^By[ \t]+([^\n]{3,100})$',header)
        result.append(dict(id=section_id,document_id=document['id'],title=title,source_title=document['title'],
            author=(credit[1].strip()+'; Ralph Mahoney (editor)' if credit else
                    'Ralph Mahoney (editor); individual contributors as credited'),publisher='World MAP',
            source_section=first,source_end_section=last,purpose='Bible study / leadership teaching',family='staff',
            ranges=ranges,text=body,blocks=_staff_reader_blocks(_study_source_blocks(body,styles),title) if with_blocks else []))
    return result


def _sermon_sections(document, rows, styles=None, *, with_blocks=True):
    """Recognize printed entry boundaries and keep all continuation pages."""
    family = _sermon_family(document['title'])
    if not family or not rows:
        return []
    if family=='pulpit':return _pulpit_sections(document,rows,styles,with_blocks=with_blocks)
    if family=='staff':return _staff_sections(document,rows,styles,with_blocks=with_blocks)
    pages = {}
    for page, content in rows:
        pages.setdefault(page, []).append(content)
    pieces, locations, toc, toc_pages, text_size = [], [], {}, set(), 0
    early_text = ''
    for page, chunks in pages.items():
        body = _join_index_chunks(chunks).strip()
        if len(early_text) < 100000:
            early_text += '\n' + body
        if len(locations) < 30 or re.search(r'TABLE OF CONTENTS|Table of Contents',body):
            numbered = list(re.finditer(r'(?<!\w)(\d{1,3})[.)]\s+', body))
            consecutive = len(numbered) >= 9 and all(int(right[1]) == int(left[1])+1 for left,right in zip(numbered,numbered[1:]))
            short_titles = sum((numbered[i+1].start() if i+1<len(numbered) else len(body))-match.end()<180 for i,match in enumerate(numbered))
            if re.search(r'TABLE OF CONTENTS|Table of Contents', body) or (consecutive and short_titles >= len(numbered)*.9):
                toc_pages.add(page)
                for index, match in enumerate(numbered):
                    end = numbered[index+1].start() if index+1 < len(numbered) else len(body)
                    title = re.sub(r'[._]{3,}.*', '', body[match.end():end]).strip()
                    if re.match(r'^52\s+(?:funeral\s+)?sermons\b',document['title'],re.I):
                        title=title.splitlines()[0] if title else ''
                    if not re.match(r'^52\s+(?:funeral\s+)?sermons\b',document['title'],re.I):
                        title = re.sub(r'\s+\d+\s*$', '', title).strip()
                    if 2 <= len(title) <= 160 and '\n' not in title:
                        toc.setdefault(int(match[1]), title)
                for line in body.splitlines():
                    match = re.match(r'\s*(?:\d+[.)]\s+)?(.+?)[._]{3,}\s*\d+', line)
                    if match:
                        toc.setdefault(_text(match[1]).casefold(), _text(match[1]))
        locations.append((text_size, page))
        pieces.append(body + '\n\n');text_size += len(pieces[-1])
    text = ''.join(pieces)
    if len(text) > 35_000_000:
        return []
    printed_author = re.search(r'(?im)^By\s*\n\s*([^\n]{3,100})', early_text)
    author = (printed_author[1].strip() if printed_author else document.get('author', ''))
    if family=='spurgeon':author='Charles Spurgeon; edited by Barry L. Davis'
    if author in ('Unknown Author', 'APEO Resources', 'Semon Help ebook', ''):
        printed = re.search(r'\bBy\s+(Barry L\. Davis[^\n\u00a0]{0,15})', early_text)
        author = printed[1].strip() if printed else 'Author as credited in the source'

    starts = []
    def add(position, title, prefix_end=None):
        # A multiline heading regex can consume the previous page's blank tail.
        position = re.compile(r'\s*').match(text,position).end()
        page = next((page for offset, page in reversed(locations) if offset <= position), locations[0][1])
        if page not in toc_pages:
            starts.append(dict(start=position, title=_text(title), page=page,
                               prefix_end=prefix_end if prefix_end is not None else position+len(title)))

    if family == 'nicoll':
        for match in re.finditer(r'(?m)^\s*([^\n]{3,100})\s*$', text):
            line = match[1].strip()
            if ':' not in line: continue
            refs = scripture_references(line)
            if len(refs) == 1 and refs[0]['start'] == 0 and refs[0]['end'] == len(line):
                add(match.start(), line, match.end())
    else:
        if family == 'apeo':
            for match in re.finditer(r'(?m)^\s*Chapter\s+\d+\s*\n\s*([^\n]{3,120})\s*$', text):
                add(match.start(),match[1],match.end())
        # Rob Westbrook prints the title immediately above the series/sermon line.
        for match in re.finditer(r'(?m)^([^\n]+)\n\s*Colossians Series - Sermon \d+\s*\n', text):
            add(match.start(), match[1], match.end())
        # Numbered entries: contents titles disambiguate inline EPUB title/text.
        for match in re.finditer(r'(?m)^\s*(?:Notes\s*)?(\d{1,3})\.\s+([^\n]+)', text):
            number, line = int(match[1]), match[2]
            title = toc.get(number)
            printed=re.match(r'\s+'.join(re.escape(word) for word in title.split()),text[match.start(2):],re.I) if title else None
            if title and not printed:title=None
            if title:
                add(match.start(), title, match.start(2)+printed.end())
            elif (3 <= len(line) <= 150 and line.isupper() and not toc
                  and not line.startswith(('COPYRIGHT', 'TABLE OF'))):
                end=match.end();title=line
                if family=='apeo':
                    for continuation in text[end:end+300].splitlines():
                        continuation=continuation.strip()
                        if not continuation:continue
                        if scripture_references(continuation) or not continuation.isupper() or len(continuation)>100:break
                        if continuation in ('NOTES','INTRODUCTION') or re.search('SERMON RESOURCE|SERMONS FROM',continuation):break
                        title+=' '+continuation
                        end=text.find(continuation,end)+len(continuation)
                        if len(title)>200:break
                add(match.start(), title, end)
        # Unnumbered all-caps titles in 130 Free Sermon Outlines and APEO chapters.
        for title in [value for key, value in toc.items() if isinstance(key, str)]:
            pattern = r'(?m)^\s*(?:Notes\s*)?' + r'\s+'.join(re.escape(word) for word in title.split()) + r'\s*$'
            for match in re.finditer(pattern, text, re.I):
                add(match.start(), title, match.end())
        if family == 'apeo':
            # Theme followed by its foundational passage, including wrapped
            # titles and unnumbered messages in otherwise numbered collections.
            # Restrict this recognizer to source page openings.
            for page_number,chunks in pages.items():
                source_lines=[line.strip() for line in _join_index_chunks(chunks).splitlines() if line.strip()]
                page_start=next(offset for offset,page in locations if page==page_number)
                body=text[page_start:]
                for number,line in enumerate(source_lines[:10]):
                    refs=scripture_references(line)
                    if not refs or refs[0]['start']>2 or refs[-1]['end']<len(line.rstrip(' .'))-8:continue
                    if number==0:continue
                    title=source_lines[number-1]
                    if re.search(r'SERMON RESOURCE|SERMONS FROM',title):continue
                    if title.casefold()=='notes' or re.fullmatch(r'\(?[A-Z]{2,5}\)?',title):continue
                    if not 3<=len(title)<=160 or title.endswith(('.',':',';')) or scripture_references(title):continue
                    if not title.isupper():continue
                    title=re.sub(r'^\d+[.)]\s*','',title)
                    position=body.find(source_lines[number-1])
                    if position>=0 and not any(entry['start']<=page_start+position<entry['prefix_end'] for entry in starts):
                        add(page_start+position,title,page_start+body.find(line,position))
            for match in (re.finditer(r'(?m)^\s*(?:Notes\s*)?(?:Chapter\s+\d+\s*\n)?([A-Z][A-Za-z ’\'",:–—-]{3,100})\s*\n', text) if not starts else ()):
                following = text[match.end():match.end()+250]
                if re.match(r'\s*(?:[“"\u2018].*?\n)?\s*(?:[1-3]\s*)?[A-Z][a-z]+\s+\d+[:.]', following):
                    add(match.start(), match[1], match.end())
    # Multiple recognizers can point at the same printed heading.
    unique = {}
    for entry in sorted(starts, key=lambda entry: entry['start']):
        if unique and entry['start'] - next(reversed(unique)) < 12:
            continue
        unique.setdefault(entry['start'], entry)
    starts = list(unique.values())
    sections = []
    for index, entry in enumerate(starts):
        end = starts[index+1]['start'] if index+1 < len(starts) else len(text)
        if family=='collection' and re.match(r'^52\s+(?:funeral\s+)?sermons\b',document['title'],re.I):
            # A closing contents page or publisher advertisement is not part of
            # the final sermon. Keep the bonus sermon where the source has one.
            later_contents=[offset for offset,page in locations if page in toc_pages and offset>entry['start']]
            if later_contents:end=min(end,min(later_contents))
            footer=re.search(r'(?im)^\s*(?:THANK YOU FOR\s+)?INVESTING IN THIS BOOK!',text[entry['start']:end])
            if footer:end=entry['start']+footer.start()
        body = text[entry['start']:end].strip()
        opening = text[entry['prefix_end']:min(end, entry['prefix_end']+1400)]
        if family == 'nicoll':
            ranges = _sermon_primary_ranges(entry['title'])
        else:
            # Stop before the first numbered/lettered exposition heading.
            boundary = re.search(r'(?m)^\s*(?:[IVX]+[.)]|[A-Z][.)]|\d+[.)])\s+', opening)
            primary = opening[:boundary.start()] if boundary and boundary.start() > 0 else opening
            ranges = _sermon_primary_ranges(primary)
            if family=='collection' and re.match(r'^52\s+(?:funeral\s+)?sermons\b',document['title'],re.I):
                # Some volumes print only a whole chapter beneath the theme.
                first_line=opening.strip().splitlines()[0] if opening.strip() else ''
                printed=_commentary_ranges(first_line)
                if printed:ranges=printed
                else:
                    illustration=re.search(r'\bILLUSTRATION:',primary,re.I)
                    if illustration:ranges=_sermon_primary_ranges(primary[:illustration.start()])
                if not ranges:
                    # About Jesus often gives its preaching text under the first
                    # outline point, after a long illustration. Use that printed
                    # quotation, never incidental references in the illustration.
                    preaching=body[max(0,entry['prefix_end']-entry['start']):]
                    point=re.search(r'(?m)^\s*1[.)]\s+[^\n]+\n',preaching)
                    if point:
                        quotation=preaching[point.end():].split('\n',1)[0]
                        ranges=_sermon_primary_ranges(quotation)
        if not ranges or len(body) < 100:
            continue
        section_id = hashlib.sha256(json.dumps([document['id'], entry['start'], body], ensure_ascii=False).encode()).hexdigest()[:32]
        purpose = 'Funeral message' if 'funeral' in document['title'].casefold() else 'Sermon outline'
        if entry['title'].casefold().startswith('for '):
            purpose += ' · ' + entry['title']
        blocks=_study_source_blocks(body,styles) if with_blocks else []
        if family=='apeo':
            blocks=_apeo_reader_blocks(blocks,entry['title'])
        if family=='spurgeon':blocks=_spurgeon_reader_blocks(blocks,entry['title'])
        if family=='collection':blocks=_collection_reader_blocks(blocks,entry['title'])
        sections.append(dict(id=section_id, document_id=document['id'], title=entry['title'],
            source_title="Spurgeon's Sermon Outlines" if family=='spurgeon' else document['title'], author=author, source_section=entry['page'],
            source_end_section=next((page for offset,page in reversed(locations) if offset<end),entry['page']),
            purpose=purpose, family=family, ranges=ranges, text=body,blocks=blocks,
            publisher='APEO / Asia Pacific Education Office' if family == 'apeo' else 'World MAP' if family == 'staff' else ''))
    return sections


@_persisted_catalogue('sermons')
def _sermon_catalogue(path, documents):
    sections, seen = [], set()
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro', uri=True, timeout=5)) as conn:
        for encoded in documents:
            document = json.loads(encoded)
            rows,styles = _indexed_source(conn,document,with_styles=False)
            for section in _sermon_sections(document, rows,styles,with_blocks=False):
                # Preserve genuinely different sermons while collapsing duplicate copies.
                signature = (_text(section['title']).casefold(), hashlib.sha256(_text(section['text']).encode()).hexdigest())
                if signature not in seen:
                    seen.add(signature); sections.append(section)
    return sections


_SERMON_CATALOGUE_LOCK = threading.RLock()


def sermon_catalogue(ai_database):
    path = Path(ai_database).resolve()
    if not path.is_file():
        return []
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=5)) as conn:
        conn.row_factory = sqlite3.Row
        documents = conn.execute('''SELECT id,title,author,format,checksum,modified_time,indexed_at,chunk_count
            FROM pij_library_documents WHERE source_type='public_ebook' AND searchable=1 AND chunk_count>0
            ORDER BY CASE WHEN lower(format)='epub' THEN 0 ELSE 1 END,id''').fetchall()
        documents=_source_documents(conn,documents)
    unique = {}
    for row in documents:
        if not _sermon_family(row['title']):
            continue
        key = re.sub(r'\s*\(.*|\bunprotected\b|\bunlocked\b', '', row['title'], flags=re.I)
        key = re.sub(r'[^a-z0-9]', '', key.casefold())
        unique.setdefault(key, json.dumps(dict(row), sort_keys=True))
    with _SERMON_CATALOGUE_LOCK:
        return _sermon_catalogue(str(path), tuple(unique.values()))


def sermons_for_chapter(ai_database, book_id, chapter):
    matches = [item for item in sermon_catalogue(ai_database) if
        item.get('source_chapter')==dict(book_id=book_id,chapter=chapter) or any(
            span['book_id'] == book_id and span['chapter'] <= chapter <= span['end_chapter'] for span in item['ranges'])]
    matches.sort(key=lambda item: (item['family'] == 'nicoll', item['title'].casefold(), item['source_title'].casefold(), item['id']))
    return [dict(id=item['id'], title=item['title']) for item in matches]


def prepare_reader_catalogues(ai_database):
    """Operator preparation avoids the initial source parse in a reading request."""
    path=Path(ai_database).resolve()
    if not path.is_file():raise ValueError('The existing AI index is required.')
    report={}
    start=time.perf_counter();prepared=0
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as conn:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='pij_library_source_formats'").fetchone():
            for doc_id,checksum,modified,version in conn.execute('''SELECT f.document_id,f.checksum,f.modified_time,f.version
                    FROM pij_library_source_formats f JOIN pij_library_documents d ON d.id=f.document_id
                    WHERE d.source_type='public_ebook' AND d.searchable=1 AND f.checksum=d.checksum AND f.modified_time=d.modified_time'''):
                fingerprint=(checksum,modified,checksum,modified,version)
                if not _source_page_path(path,doc_id,fingerprint).is_file():
                    row=conn.execute('SELECT sections_json FROM pij_library_source_formats WHERE document_id=?',(doc_id,)).fetchone()
                    _store_source_pages(path,doc_id,fingerprint,json.loads(row[0]))
                prepared+=1
    report['source_pages']=dict(documents=prepared,seconds=round(time.perf_counter()-start,3))
    for name,provider in (('wiersbe',wiersbe_commentary),('exalting',exalting_commentary),
                          ('moody',moody_commentary),('pulpit',pulpit_commentary)):
        start=time.perf_counter();result=provider(path,43,3,16)
        report[name]=dict(available=result['available'],seconds=round(time.perf_counter()-start,3))
    start=time.perf_counter();sections=sermon_catalogue(path)
    report['sermons']=dict(sections=len(sections),seconds=round(time.perf_counter()-start,3))
    return report


def _maxwell_document(ai_database):
    path = Path(ai_database).resolve()
    if not path.is_file():
        return None
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=2)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('''SELECT id,title,checksum,modified_time,indexed_at,chunk_count FROM pij_library_documents
            WHERE source_type='public_ebook' AND searchable=1 AND lower(title) LIKE '%maxwell%leadership%bible%'
            AND lower(title) LIKE '%3rd edition%' ''').fetchall()
    rows = [row for row in rows if re.search(r'\bniv\b', row['title'], re.I)]
    if len(rows) != 1:
        return None
    document = dict(rows[0])
    document['fingerprint'] = hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()
    return document


def _join_index_chunks(chunks):
    result = ''
    for content in chunks:
        # The public index overlaps successive chunks. Remove only a proven exact
        # overlap, retaining article headings split at a chunk boundary.
        overlap = 0
        prefix = content[:60]
        found = result.rfind(prefix, max(0, len(result)-2000)) if len(prefix)==60 else -1
        if found >= 0 and content.startswith(result[found:]):
            overlap = len(result)-found
        result += content[overlap:] if overlap else '\n'+content
    return result


def find_maxwell_note(ai_database, document_id, reference):
    wanted = scripture_references(reference)
    if len(wanted)!=1:
        return None
    wanted = wanted[0]
    with closing(sqlite3.connect(Path(ai_database).resolve().as_uri()+'?mode=ro', uri=True, timeout=2)) as conn:
        rows = conn.execute('SELECT page_start,content FROM pij_library_chunks WHERE document_id=? ORDER BY chunk_number', (document_id,)).fetchall()
    sections = {}
    total = 0
    for section, content in rows:
        total += len(content)
        if total > 30_000_000:
            return None
        sections.setdefault(section, []).append(content)
    matches = []
    for section, chunks in sections.items():
        text = _join_index_chunks(chunks)
        boundaries = list(re.finditer(r'Go to the (?:Index of Leadership Articles|Profiles in Leadership Index)\s*\.', text))
        start = 0
        for boundary in boundaries:
            article = text[start:boundary.start()]
            start = boundary.end()
            lines = [line.strip() for line in article.splitlines() if line.strip()]
            for index, line in enumerate(lines):
                citations = scripture_references(line)
                if index==0 or len(citations)!=1 or citations[0]['start']!=0 or citations[0]['end']!=len(line):
                    continue
                citation = citations[0]
                if (citation['book_id'] != wanted['book_id'] or
                        (citation['chapter'], citation['verse']) > (wanted['chapter'], wanted['verse']) or
                        (citation['end_chapter'], citation['end_verse']) < (wanted['end_chapter'], wanted['end_verse'])):
                    continue
                title, body = lines[index-1], '\n'.join(lines[index+1:])
                if not 20 <= len(body) <= 20000 or len(title)>250:
                    continue
                width = (citation['end_chapter']-citation['chapter'])*200 + citation['end_verse']-citation['verse']
                matches.append((width, dict(title=title, passage=line, body=body, section=section)))
    return min(matches, key=lambda item: item[0])[1] if matches else None


def _generate_maxwell_insight(path, ai_database, key, claim, document, reference, verse_text):
    status, result = 'no_match', {}
    try:
        note = find_maxwell_note(ai_database, document['id'], reference)
        if note:
            status = 'ready'
            body = note['body']
            excerpt = body.split('\n', 1)[0]
            if len(excerpt) > 600:
                excerpt = excerpt[:600].rsplit(' ', 1)[0]
            result = dict(excerpt=excerpt, content=body, title=note['title'],
                          passage=note['passage'], source_title=document['title'])
    except Exception:
        status = 'failed'
        logging.getLogger(__name__).exception('Maxwell daily insight could not be prepared')
    try:
        with closing(sqlite3.connect(path.as_uri()+'?mode=rw', uri=True, timeout=5)) as conn, conn:
            conn.execute('''UPDATE bible_daily_insights SET status=?,result_json=?,lease_until=0,updated_at=?
                WHERE cache_key=? AND status='pending' AND result_json=?''',
                (status, json.dumps(result), datetime.now(timezone.utc).isoformat(), key, claim))
    except sqlite3.DatabaseError:
        logging.getLogger(__name__).exception('Maxwell daily insight cache could not be saved')


def daily_maxwell_insight(database_path, ai_database, reference, verse_text, day):
    """Return (insight or None, pending). Missing notes/errors stay invisible.

    AI index is read-only. A daily/source-version cache and a cross-worker lease
    in the existing Bible database prevent duplicate source loading per visitor.
    Schema creation is still restricted to explicit maintenance/sync actions.
    """
    conn = None
    try:
        path = _database_path(database_path, protected_paths=(ai_database,))
        if not path.is_file() or not verse_text or 'temporarily unavailable' in verse_text:
            return None, False
        document = _maxwell_document(ai_database)
        if not document:
            return None, False
        key = hashlib.sha256(json.dumps([str(day), reference, verse_text, document['fingerprint'], 'maxwell-devotional-original-v3']).encode()).hexdigest()
        conn = sqlite3.connect(path.as_uri()+'?mode=rw', uri=True, timeout=2)
        if (conn.execute('PRAGMA application_id').fetchone()[0]!=APPLICATION_ID
                or conn.execute('PRAGMA user_version').fetchone()[0] not in (2, 3, 4, 5, 6, SCHEMA_VERSION)):
            return None, False
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('SELECT status,lease_until,result_json FROM bible_daily_insights WHERE cache_key=?', (key,)).fetchone()
        if row:
            if row[0]=='ready':
                return json.loads(row[2]), False
            if row[0] in {'no_match','failed'}:
                return None, False
            if row[1]>time.time():
                return None, True
        claim = json.dumps(dict(claim=secrets.token_hex(16)))
        conn.execute('''INSERT INTO bible_daily_insights(cache_key,status,lease_until,result_json,updated_at)
            VALUES(?,'pending',?,?,?) ON CONFLICT(cache_key) DO UPDATE SET status='pending',
            lease_until=excluded.lease_until,result_json=excluded.result_json,updated_at=excluded.updated_at''',
            (key, time.time()+1800, claim, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        threading.Thread(target=_generate_maxwell_insight,
                         args=(path, ai_database, key, claim, document, reference, verse_text),
                         name='maxwell-daily-insight', daemon=True).start()
        return None, True
    except (OSError, ValueError, sqlite3.DatabaseError, RuntimeError):
        logging.getLogger(__name__).debug('Maxwell insight unavailable', exc_info=True)
        return None, False
    finally:
        if conn is not None:
            conn.close()


def initialize_study_saves(database_path, backup_directory=None):
    """Explicit additive upgrade with a consistent recovery copy of existing data."""
    path=_database_path(database_path)
    if not path.is_file():
        raise ValueError('Initialize/import the private Bible database first.')
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as source:
        if source.execute('PRAGMA application_id').fetchone()[0]!=APPLICATION_ID:
            raise ValueError('Refusing to modify an unrecognized database.')
        if source.execute("SELECT 1 FROM sqlite_master WHERE name='bible_study_saves'").fetchone():
            return dict(status='unchanged',message='Study-save storage is already initialized.')
        directory=Path(backup_directory).resolve() if backup_directory else path.parent/'backups'
        if directory.is_relative_to(BASE_DIR/'static'):
            raise ValueError('Recovery backups must remain outside public static files.')
        directory.mkdir(parents=True,exist_ok=True)
        backup=directory/('before_study_saves_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+secrets.token_hex(3)+'.db')
        with closing(sqlite3.connect(backup)) as target:source.backup(target)
    initialize_database(path)
    return dict(status='completed',backup=str(backup),message='Study saves initialized; existing reader records retained.')


def initialize_database(database_path, **path_options):
    """Explicit schema creation for maintenance or an authorized sync action."""
    path = _database_path(database_path, **path_options)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=15)
    try:
        # Rebuild only the edition constraint during the explicit v7 upgrade.
        # Foreign-key names stay unchanged; validate every reference afterward.
        conn.execute('PRAGMA foreign_keys=OFF')
        application_id = conn.execute('PRAGMA application_id').fetchone()[0]
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
        if application_id not in (0, APPLICATION_ID) or (tables and application_id != APPLICATION_ID):
            raise ValueError('Refusing to migrate an unrecognized database.')
        version = conn.execute('PRAGMA user_version').fetchone()[0]
        if version > SCHEMA_VERSION:
            raise ValueError('This Bible Study database requires a newer application version.')
        if version == SCHEMA_VERSION and application_id == APPLICATION_ID:
            # This function is an explicit maintenance/sync entry point, never a reader request.
            with conn:
                for statement in STUDY_SAVE_SCHEMA_SQL.split(';'):
                    if statement.strip(): conn.execute(statement)
            return path
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('BEGIN IMMEDIATE')
        if version == 0:
            for statement in SCHEMA_SQL.split(';'):
                if statement.strip():
                    conn.execute(statement)
            conn.executemany('INSERT INTO bible_books(id,name,chapter_count) VALUES(?,?,?)',
                             [(index, name, count) for index, (name, count) in enumerate(BOOKS, 1)])
            conn.execute("INSERT INTO bible_metadata(key,value) VALUES('offset_unit','unicode_codepoint')")
        elif version not in (1, 2, 3, 4, 5, 6) or application_id != APPLICATION_ID:
            raise ValueError('Unsupported Bible Study migration; existing records were kept.')
        migrations = ((DICTIONARY_SCHEMA_SQL if version < 2 else '')
                      + (ORIGINAL_SCHEMA_SQL if version < 3 else '')
                      + (THOMPSON_SCHEMA_SQL if version < 4 else '')
                      + (CONCORDANCE_SCHEMA_SQL if version < 5 else '')
                      + (STUDY_SCHEMA_SQL if version < 6 else '') + STUDY_SAVE_SCHEMA_SQL)
        for statement in migrations.split(';'):
            if statement.strip():
                conn.execute(statement)
        if version != 0 and version < 7:
            definition = re.search(r'CREATE TABLE bible_editions \(.*?\);', SCHEMA_SQL, re.S)[0]
            conn.execute(definition.replace('CREATE TABLE bible_editions (','CREATE TABLE bible_editions_v7 ('))
            conn.execute('INSERT INTO bible_editions_v7 SELECT * FROM bible_editions')
            conn.execute('DROP TABLE bible_editions')
            conn.execute('ALTER TABLE bible_editions_v7 RENAME TO bible_editions')
            conn.execute('CREATE UNIQUE INDEX bible_active_translation ON bible_editions(translation) WHERE is_active=1')
            for table, column in (('bible_verses','verse_end INTEGER NOT NULL DEFAULT 0'),
                                  ('bible_verses',"notes_json TEXT NOT NULL DEFAULT '[]'"),
                                  ('bible_verses',"layout TEXT NOT NULL DEFAULT ''"),
                                  ('bible_tokens',"normalized_text TEXT NOT NULL DEFAULT ''")):
                if column.split()[0] not in {row[1] for row in conn.execute('PRAGMA table_info('+table+')')}:
                    conn.execute('ALTER TABLE '+table+' ADD COLUMN '+column)
            conn.execute('CREATE INDEX IF NOT EXISTS bible_token_word ON bible_tokens(normalized_text,verse_id)')
        if conn.execute('PRAGMA foreign_key_check').fetchone():
            raise ValueError('Bible Study migration failed foreign-key validation.')
        if version < 4:
            rebuild_dictionary_senses(conn)
        conn.execute(f'PRAGMA application_id={APPLICATION_ID}')
        conn.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
        conn.commit()
        return path
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _account_key():
    # Match Pastor's Resources permissions. A username alone is not a login.
    if session.get('pastor_logged_in') is not True and session.get('ao_logged_in') is not True:
        return ''
    return current_reader_user_key()


def _personal_bible_items(conn, user_key, book_id=None, chapter=None, translation=None):
    """Read private verse anchors without changing either reader's storage."""
    filters, values = ['p.user_key=?'], [user_key]
    if book_id is not None:
        filters.append('v.book_id=?'); values.append(book_id)
    if chapter is not None:
        filters.append('v.chapter=?'); values.append(chapter)
    if translation is not None:
        filters.append('e.translation=?'); values.append(translation)
    suffix = ''' FROM {table} p JOIN bible_verses v ON v.id=p.verse_id
        JOIN bible_books b ON b.id=v.book_id JOIN bible_editions e ON e.id=v.edition_id
        JOIN bible_import_runs r ON r.id=e.import_run_id WHERE ''' + ' AND '.join(filters)
    fields = '''p.*,v.book_id,v.chapter,v.verse,v.edition_id,b.name AS book,
        e.title AS edition_title,e.translation,e.is_active AS edition_active,v.is_active AS verse_active,
        e.status AS edition_status,r.status AS import_status'''

    def decorate(row, kind):
        reference = f"{row['book']} {row['chapter']}:{row['verse']}"
        item = dict(row)
        item.update(source='bible', format='BIBLE', reference=reference,
                    book_title='Bible · ' + reference, book_author=row['edition_title'],
                    available=bool(row['edition_active'] and row['verse_active']
                                   and row['edition_status'] == 'ready' and row['import_status'] == 'validated'))
        item.pop('user_key', None)
        item['jump_url'] = url_for('bible_study.page', **{kind[:-1]: row['id']})
        item['delete_url'] = url_for('bible_study.personal_item_api', kind=kind, item_id=row['id'])
        return item

    groups = {}
    for row in conn.execute('SELECT ' + fields + suffix.format(table='bible_annotations')
                            + ' ORDER BY v.book_id,v.chapter,v.verse,p.start_offset,p.id', values):
        item = groups.setdefault(row['group_id'], decorate(row, 'annotations'))
        item.setdefault('ranges', []).append({key: row[key] for key in
            ('verse_id', 'start_offset', 'end_offset', 'selected_text', 'book', 'chapter', 'verse', 'edition_id')})
        item['available'] = item['available'] and bool(row['edition_active'] and row['verse_active']
            and row['edition_status'] == 'ready' and row['import_status'] == 'validated')
    annotations = list(groups.values())
    for item in annotations:
        item['annotation_type'] = item['kind']; item['is_sermon_note'] = False
        item['selected_text'] = '\n'.join(span['selected_text'] for span in item['ranges'])
        last = item['ranges'][-1]
        if len(item['ranges']) > 1:
            item['reference'] += ('–' + str(last['verse']) if last['book'] == item['book']
                and last['chapter'] == item['chapter'] else f" – {last['book']} {last['chapter']}:{last['verse']}")
            item['book_title'] = 'Bible · ' + item['reference']
    bookmarks = [decorate(row, 'bookmarks') for row in conn.execute(
        'SELECT ' + fields + suffix.format(table='bible_bookmarks') + ' ORDER BY p.updated_at DESC,p.id', values)]
    annotations.sort(key=lambda item: (item['updated_at'], item['id']), reverse=True)
    return dict(annotations=annotations, bookmarks=bookmarks)


def bible_personal_library(app, user_key):
    """Bridge Bible saves into My Library; absent storage stays absent."""
    empty = dict(annotations=[], bookmarks=[])
    if not user_key:
        return empty
    try:
        path = _database_path(app.config.get('BIBLE_STUDY_DATABASE', BASE_DIR / 'bible_study_data' / 'bible_study.db'),
            static_folder=app.static_folder, protected_paths=(app.config.get('DATABASE'), app.config.get('AI_DATABASE')))
        if not path.is_file():
            return empty
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as conn:
            conn.row_factory = sqlite3.Row
            if (conn.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID
                    or conn.execute('PRAGMA user_version').fetchone()[0] not in range(1, SCHEMA_VERSION + 1)):
                return empty
            items = _personal_bible_items(conn, user_key)
            study = _study_saved_items(conn, user_key)
            for kind in ('annotations', 'bookmarks'): items[kind].extend(study[kind])
            return items
    except (ValueError, OSError, sqlite3.DatabaseError):
        logging.getLogger(__name__).warning('Private Bible collections are unavailable')
        return empty


def _study_saved_items(conn, user_key):
    items = dict(annotations=[], bookmarks=[])
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='bible_study_saves'").fetchone():
        return items
    groups = {}
    for row in conn.execute('SELECT * FROM bible_study_saves WHERE user_key=? ORDER BY created_at,id', (user_key,)):
        source = json.loads(row['source_json'])
        item = groups.setdefault(row['group_id'], dict(id=row['id'], group_id=row['group_id'], source='bible',
            format='STUDY', kind=row['kind'], annotation_type=row['kind'], is_sermon_note=False,
            reference=source['label'], book_title=source['label'], book_author=source['title'],
            color=row['color'], note=row['note'], version=row['version'], created_at=row['created_at'], updated_at=row['updated_at'],
            jump_url=url_for('bible_study.page', study=row['id']),
            delete_url=url_for('bible_study.study_saved_item_api', item_id=row['id']), ranges=[]))
        item['ranges'].append(dict(source_key=row['source_key'], source=source, block_text=row['block_text'],
            token=URLSafeSerializer(current_app.secret_key,salt='bible-study-text-v1').dumps(source),
            start_offset=row['start_offset'], end_offset=row['end_offset'], selected_text=row['selected_text']))
    for item in groups.values():
        item['selected_text'] = '\n'.join(span['selected_text'] for span in item['ranges'])
        items['bookmarks' if item['kind'] == 'bookmark' else 'annotations'].append(item)
    return items


def register_bible_study_routes(app):
    app.config.setdefault('BIBLE_STUDY_DATABASE', str(BASE_DIR / 'bible_study_data' / 'bible_study.db'))
    app.config.setdefault('AI_DATABASE', os.getenv('AI_DATABASE', str(BASE_DIR / 'ai_index.db')))
    app.config.setdefault('BIBLE_STUDY_TRANSLATION_FOLDER_ID', resources.BIBLE_TRANSLATIONS_DRIVE_FOLDER_ID)
    blueprint = Blueprint('bible_study', __name__, url_prefix='/pastor-resources/bible-study')

    def available():
        return bool(_account_key())

    def sync_admin():
        return session.get('ao_logged_in') is True and is_resource_admin()

    def configured_path():
        return _database_path(app.config['BIBLE_STUDY_DATABASE'], static_folder=app.static_folder,
                              protected_paths=(app.config.get('DATABASE'), app.config.get('AI_DATABASE'),
                                               app.config.get('BIBLE_STUDY_LIBRARY_DATABASE')))

    @contextmanager
    def database(write=False):
        conn = None
        try:
            path = configured_path()
            if not path.is_file():
                raise StorageUnavailable('not_initialized')
            # mode=rw/ro deliberately prevents implicit database creation.
            conn = sqlite3.connect(path.as_uri() + ('?mode=rw' if write else '?mode=ro'), uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('PRAGMA busy_timeout=5000')
            if (conn.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID
                    or conn.execute('PRAGMA user_version').fetchone()[0] not in (1, 2, 3, 4, 5, 6, SCHEMA_VERSION)):
                raise StorageUnavailable('not_initialized')
            yield conn
        except ValueError:
            raise StorageUnavailable('configuration_error') from None
        except sqlite3.DatabaseError:
            raise StorageUnavailable('storage_unavailable') from None
        finally:
            if conn is not None:
                conn.close()

    def storage_status():
        try:
            with database() as conn:
                verse_count = conn.execute('''SELECT COUNT(*) FROM bible_verses v
                    JOIN bible_editions e ON e.id=v.edition_id JOIN bible_import_runs r ON r.id=e.import_run_id
                    WHERE e.translation IN ('NIV','CEBOCB','TCB') AND e.is_active=1 AND e.status='ready'
                    AND r.status='validated' AND v.is_active=1''').fetchone()[0]
            return {'status': 'schema_ready', 'schema_ready': True, 'indexed_verses': verse_count,
                    'message': (f'{verse_count:,} Bible verse entries are available.' if verse_count else
                                'No Bible text has been imported. A developer can use Sync Bible Study.')}
        except StorageUnavailable as error:
            status = str(error)
            messages = {
                'not_initialized': 'Bible Study storage has not been initialized. No Bible text has been imported.',
                'configuration_error': 'Bible Study storage needs developer configuration.',
                'storage_unavailable': 'Bible Study storage is temporarily unavailable. Please retry later.',
            }
            return {'status': status, 'schema_ready': False, 'indexed_verses': 0,
                    'message': messages.get(status, messages['storage_unavailable'])}

    def default_state():
        return {'book': 'Genesis', 'chapter': 1, 'verse': 1, 'translation': 'NIV', 'theme': 'paper', 'font_size': 19,
                'layout_mode': 'split', 'split_ratio': 60, 'version': 0}

    @blueprint.before_request
    def protect_routes():
        if not _account_key():
            if request.endpoint == 'bible_study.page':
                return redirect(url_for('splash'))
            return jsonify(ok=False, error='Login required.'), 401
        if not app.secret_key or app.secret_key == 'change-this-secret-key-123':
            return jsonify(ok=False, error='Configure website session security before enabling Bible Study.'), 503
        if request.method in {'POST', 'PATCH', 'DELETE', 'PUT'}:
            supplied = request.headers.get('X-CSRF-Token', '')
            expected = session.get('bible_study_csrf', '')
            if (not expected or not supplied or len(supplied) > 256
                    or not hmac.compare_digest(str(expected).encode('utf-8'), supplied.encode('utf-8'))):
                return jsonify(ok=False, error='This session expired. Reload Bible Study and try again.'), 400
            limit = 131072 if request.endpoint == 'bible_study.study_save_create' else MAX_JSON_BYTES
            if request.content_length is None or request.content_length > limit:
                return jsonify(ok=False, error='The request is too large.'), 413
            if request.mimetype != 'application/json':
                return jsonify(ok=False, error='A JSON object is required.'), 415

    @blueprint.after_request
    def private_response(response):
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        categories = {'word_api':'word', 'dictionary_search_api':'word', 'dictionary_entry_api':'word',
            'person_api':'word', 'book_study_api':'book', 'commentary_api':'commentary', 'chain_api':'chain',
            'thompson_reference_api':'chain', 'concordance_api':'concordance', 'verse_api':'verse', 'sermon_api':'sermon'}
        category = categories.get((request.endpoint or '').split('.')[-1])
        if category and response.status_code == 200 and response.is_json:
            data = response.get_json()
            if isinstance(data, dict) and data.get('ok'):
                anchors, attachments = [], []
                labels = {'word':'Word Study','book':'Book Study','commentary':'Commentary','chain':'Thompson topic',
                          'concordance':'Thompson Concordance','verse':'Verse preview','sermon':'Sermon Outlines'}
                origin = request.path + ('?' + request.query_string.decode('utf-8') if request.query_string else '')
                signer = URLSafeSerializer(app.secret_key, salt='bible-study-text-v1')
                def collect(value, path=(), title=''):
                    if isinstance(value, dict):
                        volume=value.get('source_title') or value.get('label')
                        heading=value.get('title') or value.get('headword') or value.get('reference')
                        if volume:
                            title=str(volume)+(' · '+str(heading) if heading and heading!=volume else '')
                        elif heading:
                            title=(title+' · ' if title and heading!=title else '')+str(heading)
                        for key, child in value.items():
                            # The enclosing block already carries its immutable
                            # text anchor. Signing every typography run duplicates
                            # the same words, metadata and signatures hundreds of times.
                            if key=='runs':continue
                            if key in ('text','preview','headword','title','body','definition','excerpt') and isinstance(child,str) and child.strip():
                                descriptor = dict(origin=origin, path=list(path)+( [key] ), category=category,
                                    title=title[:250] or labels[category], label=labels[category],
                                    digest=hashlib.sha256(child.encode()).hexdigest(), block_type=value.get('type','paragraph'))
                                # Exact text/revision is part of the anchor. DOM styling never changes it.
                                descriptor['key'] = hashlib.sha256(json.dumps([category,title,descriptor['digest']],sort_keys=True).encode()).hexdigest()
                                anchor=dict(text=child, token=signer.dumps(descriptor), **descriptor)
                                anchors.append(anchor);attachments.append((value,key,anchor))
                            elif isinstance(child, (dict,list)):
                                collect(child, (*path,key), title)
                    elif isinstance(value, list):
                        for index, child in enumerate(value): collect(child, (*path,index), title)
                collect(data)
                for value,key,anchor in attachments:
                    value.setdefault('_study_texts',{})[key]=anchor
                data['study_anchors'] = anchors
                response.set_data(json.dumps(data, ensure_ascii=False,separators=(',',':')))
        return response

    @blueprint.get('')
    def page():
        if 'bible_study_csrf' not in session:
            session['bible_study_csrf'] = secrets.token_urlsafe(32)
        return render_template('bible_study.html', csrf_token=session['bible_study_csrf'],
                               bible_books=[{'name': name, 'chapters': count} for name, count in BOOKS],
                               bible_study_admin=sync_admin(), bible_translations=READER_TRANSLATIONS,
                               bible_preference_key=hashlib.sha256(_account_key().encode()).hexdigest()[:20])

    @blueprint.get('/api/status')
    def status_api():
        return jsonify(ok=True, **storage_status())

    @blueprint.get('/api/sync')
    def sync_status():
        if not sync_admin():
            return jsonify(ok=False, error='Developer access required.'), 403
        try:
            with database() as conn:
                row = conn.execute("SELECT value FROM bible_metadata WHERE key='sync_status'").fetchone()
                report = json.loads(row['value']) if row else {'status': 'idle', 'message': 'Ready to sync NIV, Mounce and Vine directly from For Scripture Study App.'}
            if report['status'] == 'running':
                lock = _sync_lock(configured_path())
                if lock is not None:
                    _release_sync_lock(lock)
                    report = {'status': 'interrupted', 'message': 'The previous sync was interrupted. Retry to sync safely.'}
            return jsonify(ok=True, sync=report)
        except StorageUnavailable:
            return jsonify(ok=True, sync={'status': 'idle', 'message': 'Sync will initialize the separate Bible Study database.'})
        except (ValueError, OSError):
            return jsonify(ok=False, error='Bible Study sync status is unavailable.'), 503

    @blueprint.post('/api/sync')
    def sync_start():
        if not sync_admin():
            return jsonify(ok=False, error='Developer access required.'), 403
        if request.get_json(silent=True) != {}:
            return jsonify(ok=False, error='Sync reads the official Drive folder. Send an empty JSON object.'), 400
        lock = None
        try:
            path = configured_path()
            lock = _sync_lock(path, create=True)
            if lock is None:
                return jsonify(ok=False, error='A Bible Study sync is already running.'), 409
            if not path.exists():
                initialize_database(path, static_folder=app.static_folder)
            _write_sync_status(path, {'status': 'running', 'message': 'Starting Bible Study sync.',
                                     'updated_at': datetime.now(timezone.utc).isoformat()})
            threading.Thread(target=_run_sync, args=(app, path, lock), name='bible-study-sync', daemon=True).start()
            lock = None  # The background worker now owns the cross-process lock.
            return jsonify(ok=True, message='Bible Study sync started.'), 202
        except (ValueError, OSError, sqlite3.DatabaseError, RuntimeError):
            logging.getLogger(__name__).exception('Could not start Bible Study sync')
            return jsonify(ok=False, error='Could not start sync. Check the private Bible Study database path and server logs.'), 503
        finally:
            _release_sync_lock(lock)

    def requested_reference():
        book = BOOK_BY_NAME.get(request.args.get('book', 'Genesis').strip().casefold())
        try:
            chapter = int(request.args.get('chapter', '1'))
        except ValueError:
            return None
        return (book[0], chapter) if book and 1 <= chapter <= book[2] else None

    def requested_translation():
        value = request.args.get('translation', 'NIV').upper()
        return value if value in READER_TRANSLATIONS else None

    @blueprint.get('/api/translations')
    def translations_api():
        present = set()
        try:
            with database() as conn:
                present = {row[0] for row in conn.execute('''SELECT e.translation FROM bible_editions e
                    JOIN bible_import_runs r ON r.id=e.import_run_id WHERE e.is_active=1 AND e.status='ready'
                    AND r.status='validated' ''')}
        except StorageUnavailable: pass
        return jsonify(ok=True, translations=[dict(code=key,label=value['label'],available=key in present)
                       for key,value in READER_TRANSLATIONS.items()])

    def read_verses(conn, reference, verse=None, translation='NIV'):
        columns = "v.verse_end,v.notes_json,v.layout" if conn.execute('PRAGMA user_version').fetchone()[0]>=7 else "v.verse AS verse_end,'[]' AS notes_json,'' AS layout"
        return conn.execute('''SELECT v.id,v.verse,v.text,v.heading,e.title,e.attribution,e.translation,'''+columns+'''
            FROM bible_verses v JOIN bible_editions e ON e.id=v.edition_id
            JOIN bible_import_runs r ON r.id=e.import_run_id
            WHERE e.translation=? AND e.is_active=1 AND e.status='ready' AND r.status='validated'
            AND v.is_active=1 AND v.book_id=? AND v.chapter=? AND (? IS NULL OR v.verse=?) ORDER BY v.verse''',
                            (translation, *reference, verse, verse)).fetchall()

    @blueprint.get('/api/chapter')
    def chapter_api():
        reference = requested_reference()
        translation = requested_translation()
        if reference is None or translation is None:
            return jsonify(ok=False, error='Invalid Bible book or chapter.'), 400
        try:
            with database() as conn:
                rows = read_verses(conn, reference, translation=translation)
                verses = []
                for row in rows:
                    tokens = conn.execute('SELECT token_index,start_offset,end_offset,text FROM bible_tokens WHERE verse_id=? ORDER BY token_index', (row['id'],)).fetchall()
                    spans=[dict(r) for r in conn.execute('SELECT start_offset,end_offset FROM bible_red_letter_spans WHERE verse_id=? ORDER BY start_offset',(row['id'],))]
                    topics=[];edition_label=READER_TRANSLATIONS[translation]['label']
                    if conn.execute('PRAGMA user_version').fetchone()[0]>=4:
                        topics=[dict(r) for r in conn.execute('''SELECT DISTINCT t.id,t.topic_key,t.title FROM bible_chain_verses x
                            JOIN bible_chain_topics t ON t.id=x.topic_id JOIN bible_verses nv ON nv.id=x.verse_id
                            JOIN bible_editions ne ON ne.id=nv.edition_id
                            WHERE ne.translation='NIV' AND ne.is_active=1 AND nv.is_active=1
                            AND nv.book_id=? AND nv.chapter=? AND nv.verse BETWEEN ? AND ?
                            ORDER BY CAST(t.topic_key AS INTEGER),t.topic_key''',
                            (reference[0],reference[1],row['verse'],row['verse_end'] or row['verse']))]
                        detail=conn.execute('''SELECT d.label FROM bible_verses v JOIN bible_edition_details d ON d.edition_id=v.edition_id WHERE v.id=?''',(row['id'],)).fetchone()
                        if detail:edition_label=detail[0]
                    verses.append({**dict(row), 'verse_end': row['verse_end'] or row['verse'],
                        'footnotes':json.loads(row['notes_json']), 'tokens': [dict(token) for token in tokens],
                        'red_spans':spans,'topics':topics,'edition_label':edition_label})
            return jsonify(ok=True, verses=verses, storage_available=True,translation=translation)
        except StorageUnavailable:
            return jsonify(ok=True, verses=[], storage_available=False)

    @blueprint.get('/api/verse')
    def verse_api():
        reference = requested_reference()
        translation = requested_translation()
        try:
            number = int(request.args.get('verse', '0'))
            end_chapter = int(request.args.get('end_chapter', str(reference[1] if reference else 0)))
            end_verse = int(request.args.get('end_verse', str(number)))
        except ValueError:
            number = end_chapter = end_verse = 0
        if (reference is None or translation is None or not 1 <= number <= 176 or not 1 <= end_verse <= 176
                or not reference[1] <= end_chapter <= BOOKS[reference[0]-1][1]
                or (reference[1], number) > (end_chapter, end_verse)):
            return jsonify(ok=False, error='Invalid verse reference.'), 400
        try:
            with database() as conn:
                end_column = 'COALESCE(NULLIF(v.verse_end,0),v.verse)' if conn.execute('PRAGMA user_version').fetchone()[0]>=7 else 'v.verse'
                rows = conn.execute('''SELECT v.id,v.chapter,v.verse,v.text,e.attribution,e.translation,'''+end_column+''' AS verse_end FROM bible_verses v
                    JOIN bible_editions e ON e.id=v.edition_id JOIN bible_import_runs r ON r.id=e.import_run_id
                    WHERE e.translation=? AND e.is_active=1 AND e.status='ready' AND r.status='validated'
                    AND v.is_active=1 AND v.book_id=? AND v.chapter BETWEEN ? AND ?
                    AND (v.chapter>? OR '''+end_column+'''>=?) AND (v.chapter<? OR v.verse<=?)
                    ORDER BY v.chapter,v.verse LIMIT 501''',
                    (translation,reference[0], reference[1], end_chapter, reference[1], number, end_chapter, end_verse)).fetchall()
            if not rows:
                return jsonify(ok=False, error='This verse is not present in the selected translation.'), 404
            if len(rows)>500:
                return jsonify(ok=False, error='This passage is too long for a verse preview.'), 400
            number = rows[0]['verse']; end_verse = rows[-1]['verse_end']
            first_chapter = rows[0]['chapter']; end_chapter = rows[-1]['chapter']
            label = f'{BOOKS[reference[0]-1][0]} {first_chapter}:{number}'
            if end_chapter != first_chapter or end_verse != number:
                label += '–' + (str(end_chapter)+':' if end_chapter != first_chapter else '') + str(end_verse)
            text = rows[0]['text'] if len(rows)==1 else '\n\n'.join(
                f"{row['chapter']}:{row['verse']}" + ('–'+str(row['verse_end']) if row['verse_end']>row['verse'] else '')
                + ' ' + row['text'] for row in rows)
            return jsonify(ok=True, verse={**dict(rows[0]), 'text': text, 'reference': label,
                                          'edition_label':READER_TRANSLATIONS[translation]['label']})
        except StorageUnavailable:
            return jsonify(ok=False, error='Bible Study data is unavailable.'), 503

    @blueprint.get('/api/word/<int:verse_id>/<int:token_index>')
    def word_api(verse_id, token_index):
        try:
            with database() as conn:
                token = conn.execute('''SELECT t.*,v.book_id,v.chapter,v.verse,v.content_hash FROM bible_tokens t
                    JOIN bible_verses v ON v.id=t.verse_id JOIN bible_editions e ON e.id=v.edition_id
                    JOIN bible_import_runs r ON r.id=e.import_run_id
                    WHERE t.verse_id=? AND t.token_index=? AND e.translation='NIV' AND e.is_active=1
                    AND e.status='ready' AND r.status='validated' AND v.is_active=1''', (verse_id, token_index)).fetchone()
                if not token:
                    return jsonify(ok=False, error='This indexed Bible word is unavailable.'), 404
                result = verified_word_study(conn, token)
                if not result.get('verified'):
                    result=contextual_dictionary_study(conn,token)
                people=person_study(conn,token['text'],(token['book_id'],token['chapter'],token['verse'])) if token['text'][:1].isupper() else dict(people=[],selected=None)
                return jsonify(ok=True, word=token['text'], people=people, **result)
        except StorageUnavailable:
            return jsonify(ok=False, error='Bible Study data is unavailable.'), 503

    @blueprint.get('/api/dictionaries/search')
    def dictionary_search_api():
        query = request.args.get('q', '').strip()
        family = request.args.get('source') or None
        try:
            offset = max(0, min(100000, int(request.args.get('offset', '0'))))
        except ValueError:
            return jsonify(ok=False, error='Invalid result offset.'), 400
        if not query or len(query)>100 or family not in (None, 'mounce', 'vine'):
            return jsonify(ok=False, error='Enter a word, transliteration, Strong’s number or GK number.'), 400
        try:
            with database() as conn:
                return jsonify(ok=True, people=person_study(conn,query), **search_dictionaries(conn, query, offset, family))
        except StorageUnavailable:
            return jsonify(ok=False, error='Bible Study data is unavailable.'), 503

    @blueprint.get('/api/dictionaries/entry/<int:entry_id>')
    def dictionary_entry_api(entry_id):
        try:
            with database() as conn:
                if conn.execute('PRAGMA user_version').fetchone()[0]<2:
                    return jsonify(ok=False, error='Dictionaries have not been synced yet.'), 404
                row = conn.execute('''SELECT e.*,d.family,s.title AS source_title FROM bible_dictionary_entries e
                    JOIN bible_dictionary_versions d ON d.id=e.version_id JOIN bible_sources s ON s.id=d.source_id
                    JOIN bible_import_runs r ON r.id=d.import_run_id WHERE e.id=? AND d.is_active=1 AND r.status='validated' ''', (entry_id,)).fetchone()
                if not row:
                    return jsonify(ok=False, error='This dictionary entry is unavailable. Search again after syncing.'), 404
                identifiers = conn.execute('SELECT scheme,language,number,qualifier,source_text FROM bible_dictionary_identifiers WHERE entry_id=? ORDER BY scheme,language,number', (entry_id,)).fetchall()
                blocks=json.loads(row['content_json']);language=row['language']
                if request.args.get('sense'):
                    try:sense_id=int(request.args['sense'])
                    except ValueError:return jsonify(ok=False,error='Invalid dictionary definition.'),400
                    if conn.execute('PRAGMA user_version').fetchone()[0]<4:
                        return jsonify(ok=False,error='Definition index is unavailable. Sync Bible Study.'),404
                    sense=conn.execute('SELECT * FROM bible_dictionary_senses WHERE id=? AND entry_id=?',(sense_id,entry_id)).fetchone()
                    if not sense:return jsonify(ok=False,error='Dictionary definition not found.'),404
                    blocks=blocks[sense['block_start']:sense['block_end']];language=sense['language']
                    identifiers=conn.execute('SELECT scheme,language,number,qualifier,source_text FROM bible_dictionary_sense_identifiers WHERE sense_id=?',(sense_id,)).fetchall()
                return jsonify(ok=True, entry=dict(id=row['id'], headword=row['headword'], source=row['family'],
                    source_title=row['source_title'], section=row['section'], language=language,
                    blocks=blocks, identifiers=[dict(item) for item in identifiers]))
        except StorageUnavailable:
            return jsonify(ok=False, error='Dictionary data is unavailable.'), 503

    @blueprint.get('/api/dictionaries/asset/<digest>')
    def dictionary_asset_api(digest):
        if not re.fullmatch('[a-f0-9]{64}', digest):
            return Response(status=404)
        try:
            with database() as conn:
                if conn.execute('PRAGMA user_version').fetchone()[0]<2:
                    return Response(status=404)
                row = conn.execute('''SELECT a.mime_type,a.data FROM bible_dictionary_assets a WHERE a.sha256=? AND EXISTS(
                    SELECT 1 FROM bible_dictionary_entry_assets x JOIN bible_dictionary_entries e ON e.id=x.entry_id
                    JOIN bible_dictionary_versions d ON d.id=e.version_id JOIN bible_import_runs r ON r.id=d.import_run_id
                    WHERE x.sha256=a.sha256 AND d.is_active=1 AND r.status='validated')''', (digest,)).fetchone()
            if not row:
                return Response(status=404)
            return Response(row['data'], mimetype=row['mime_type'])
        except StorageUnavailable:
            return Response(status=404)

    @blueprint.get('/api/book-study')
    def book_study_api():
        book=BOOK_BY_NAME.get(request.args.get('book','').strip().casefold())
        if not book:return jsonify(ok=False,error='Choose a valid Bible book.'),400
        try:
            with database() as conn:
                if conn.execute('PRAGMA user_version').fetchone()[0]<6:return jsonify(ok=True,available=False)
                row=conn.execute('''SELECT s.* FROM bible_thompson_studies s JOIN bible_editions e ON e.id=s.edition_id
                    JOIN bible_import_runs r ON r.id=e.import_run_id WHERE s.kind='book' AND s.book_id=?
                    AND e.is_active=1 AND e.status='ready' AND r.status='validated' ''',(book[0],)).fetchone()
                if not row:return jsonify(ok=True,available=False)
                content = json.loads(row['content_json'])
                content['blocks'] = _book_chain_blocks(conn, row['edition_id'], content['blocks'])
                return jsonify(ok=True,available=True,study=dict(title=row['title'],source_page=row['source_page'],**content))
        except StorageUnavailable:return jsonify(ok=False,error='Book study data is unavailable.'),503

    @blueprint.get('/api/commentary')
    def commentary_api():
        book = BOOK_BY_NAME.get(request.args.get('book','').strip().casefold())
        series = request.args.get('series','wiersbe')
        providers = {'wiersbe':wiersbe_commentary,'exalting-jesus':exalting_commentary,'moody':moody_commentary,'pulpit':pulpit_commentary}
        if series not in providers:
            return jsonify(ok=False,error='Choose an available commentary series.'),400
        section_kind=request.args.get('section','exposition')
        if series=='pulpit' and section_kind not in ('exposition','homiletics'):
            return jsonify(ok=False,error='Choose Exposition or Homiletics.'),400
        try:
            chapter, verse = int(request.args.get('chapter','1')), int(request.args.get('verse','1'))
        except ValueError:
            return jsonify(ok=False,error='Choose a valid Bible passage.'),400
        if not book or not 1 <= chapter <= book[2] or not 1 <= verse <= 176:
            return jsonify(ok=False,error='Choose a valid Bible passage.'),400
        try:
            arguments=(app.config.get('AI_DATABASE') or BASE_DIR / 'ai_index.db',book[0],chapter,verse)
            data = providers[series](*arguments,section_kind) if series=='pulpit' else providers[series](*arguments)
            return jsonify(ok=True,series=series,reference=f'{book[1]} {chapter}:{verse}',**data)
        except (OSError, sqlite3.DatabaseError):
            logging.getLogger(__name__).warning('Commentary index unavailable (%s)',series,exc_info=True)
            return jsonify(ok=False,error='Commentary is temporarily unavailable. Please try again.'),503

    @blueprint.get('/api/pulpit-greek')
    def pulpit_greek_api():
        spelling=request.args.get('word','').strip()
        book=BOOK_BY_NAME.get(request.args.get('book','').strip().casefold())
        chapter=request.args.get('chapter',type=int);verse=request.args.get('verse',type=int)
        if not spelling or len(spelling)>100 or not book or chapter is None or verse is None or not 1<=chapter<=book[2] or not 1<=verse<=176:
            return jsonify(ok=False,error='Choose a Greek term and a valid passage.'),400
        try:
            with database() as conn:
                return jsonify(ok=True,match=pulpit_greek_dictionary_match(conn,spelling,book[0],chapter,verse))
        except StorageUnavailable:
            return jsonify(ok=True,match=None)

    @blueprint.get('/api/sermons')
    def sermons_api():
        book = BOOK_BY_NAME.get(request.args.get('book','').casefold())
        chapter = request.args.get('chapter', type=int)
        if not book or chapter is None or not 1 <= chapter <= book[2]:
            return jsonify(ok=False,error='Choose a valid book and chapter.'),400
        try:
            items = sermons_for_chapter(app.config['AI_DATABASE'], book[0], chapter)
            return jsonify(ok=True, sermons=items, reference=f'{book[1]} {chapter}')
        except (OSError,sqlite3.DatabaseError):
            return jsonify(ok=False,error='Sermon outlines are temporarily unavailable.'),503

    @blueprint.get('/api/sermons/<sermon_id>')
    def sermon_api(sermon_id):
        try:
            item = next((item for item in sermon_catalogue(app.config['AI_DATABASE']) if item['id']==sermon_id),None)
            if not item: return jsonify(ok=False,error='This sermon is no longer available.'),404
            blocks = item.get('blocks') or _rich_source_blocks(app.config['AI_DATABASE'],item)
            return jsonify(ok=True,sermon={key:value for key,value in item.items() if key!='text'} | dict(blocks=blocks))
        except (OSError,sqlite3.DatabaseError):
            return jsonify(ok=False,error='This sermon is temporarily unavailable.'),503

    @blueprint.get('/api/thompson-reference/<key>')
    def thompson_reference_api(key):
        if not re.fullmatch(r'\d{1,4}[a-z]?', key):
            return jsonify(ok=False,error='Invalid Thompson reference.'),400
        try:
            with database() as conn:
                edition = conn.execute('''SELECT e.id FROM bible_editions e JOIN bible_import_runs r ON r.id=e.import_run_id
                    WHERE e.is_active=1 AND e.status='ready' AND r.status='validated' AND e.translation='NIV' ''').fetchone()
                if not edition or conn.execute('PRAGMA user_version').fetchone()[0] < 6:
                    return jsonify(ok=False,error='Thompson studies are unavailable.'),404
                number = int(re.match(r'\d+', key)[0])
                charts = {'4301a':'charts:abraham','4302a':'charts:moses','4302c':'charts:joshua',
                          '4302d':'charts:gideon','4302e':'charts:david','4302f':'charts:solomon',
                          '4303a':'charts:jesus','4304a':'charts:paul'}
                study_key = 'book:'+str(number-4219) if key.isdigit() and 4220 <= number <= 4285 else charts.get(key,'character:'+key)
                row = conn.execute('SELECT * FROM bible_thompson_studies WHERE edition_id=? AND study_key=?',
                                   (edition[0],study_key)).fetchone()
                if row:
                    data = json.loads(row['content_json'])
                    blocks = data.get('outline',{}).get('blocks',[]) if row['kind']=='book' and data.get('outline') else data['blocks']
                    pages = data.get('pages',[])
                    if key=='4302d':pages=[1921,1922]
                    source_page = data['outline'].get('source_page',row['source_page']) if row['kind']=='book' and data.get('outline') else row['source_page']
                    title = 'Journeys of Gideon and Samuel' if key=='4302d' else row['title']
                    return jsonify(ok=True,topic=dict(key=key,title=title,blocks=blocks,pages=pages,source_page=source_page,
                                                     source_label='Thompson study material'))
                # Some printed introductions refer to supplementary numbered
                # entries absent from this edition. Preserve the number without
                # silently substituting a different topic or inventing its text.
                return jsonify(ok=True,topic=dict(key=key,title='Thompson reference',source_page=None,
                    source_label='Thompson Chain-Reference Bible',blocks=[dict(type='paragraph',text=
                        'This printed reference points to entry '+key+', which is not included in the indexed Thompson source.',references=[])]))
        except StorageUnavailable:
            return jsonify(ok=False,error='Thompson studies are unavailable.'),503

    @blueprint.get('/api/people/<int:person_id>')
    def person_api(person_id):
        try:
            with database() as conn:
                if conn.execute('PRAGMA user_version').fetchone()[0]<6:return jsonify(ok=False,error='Person studies have not been synced.'),404
                row=conn.execute('''SELECT p.* FROM bible_person_profiles p JOIN bible_editions e ON e.id=p.edition_id
                    JOIN bible_import_runs r ON r.id=e.import_run_id WHERE p.id=? AND e.is_active=1
                    AND e.status='ready' AND r.status='validated' ''',(person_id,)).fetchone()
                if not row:return jsonify(ok=False,error='This person study is unavailable.'),404
                return jsonify(ok=True,person=_person_payload(conn,row))
        except StorageUnavailable:return jsonify(ok=False,error='Person study data is unavailable.'),503

    @blueprint.get('/api/studies/page/<int:number>')
    def study_page_api(number):
        try:
            with database() as conn:
                if conn.execute('PRAGMA user_version').fetchone()[0]<6:return Response(status=404)
                row=conn.execute('''SELECT p.image_png FROM bible_thompson_study_pages p
                    JOIN bible_editions e ON e.id=p.edition_id JOIN bible_import_runs r ON r.id=e.import_run_id
                    WHERE p.source_page=? AND e.is_active=1 AND e.status='ready' AND r.status='validated' ''',(number,)).fetchone()
                return Response(row['image_png'],mimetype='image/png') if row else Response(status=404)
        except StorageUnavailable:return Response(status=404)

    @blueprint.get('/api/study-image')
    def study_image_api():
        key=request.args.get('page','')
        if not key.isdigit():return jsonify(ok=False,error='Choose an available study page.'),400
        response=study_page_api(int(key))
        if response.status_code!=200:return jsonify(ok=False,error='Study page unavailable.'),404
        title='Thompson study page '+str(int(key))
        source=dict(origin=request.path+'?page='+str(int(key)),path=['image','text'],category='image',
            title=title,label='Thompson chart / study page',digest=hashlib.sha256(title.encode()).hexdigest(),
            block_type='paragraph',image_url=url_for('bible_study.study_page_api',number=int(key)))
        source['key']=hashlib.sha256(response.get_data()).hexdigest()
        return jsonify(ok=True,image=dict(text=title,url=source['image_url']),
            anchor=dict(text=title,token=URLSafeSerializer(app.secret_key,salt='bible-study-text-v1').dumps(source),**source))

    @blueprint.get('/api/chain/<int:topic_id>')
    def chain_api(topic_id):
        try:
            with database() as conn:
                if conn.execute('PRAGMA user_version').fetchone()[0]<4:
                    return jsonify(ok=False,error='Thompson topics have not been synced.'),404
                row=conn.execute('''SELECT t.* FROM bible_chain_topics t JOIN bible_editions e ON e.id=t.edition_id
                    JOIN bible_import_runs r ON r.id=e.import_run_id WHERE t.id=? AND e.is_active=1
                    AND e.status='ready' AND r.status='validated' ''',(topic_id,)).fetchone()
                if not row:return jsonify(ok=False,error='This chain topic is unavailable.'),404
                blocks=json.loads(row['content_json'])
                targets={r['topic_key']:dict(r) for r in conn.execute(
                    'SELECT id,topic_key,title FROM bible_chain_topics WHERE edition_id=?',(row['edition_id'],))}
                for block in blocks:
                    # Only source related-topic notes supply chain links. Never
                    # interpret a verse number or an ordinary prose number as one.
                    if block.get('role')!='related':
                        continue
                    links=[]; start=0
                    for match in re.finditer(r'(?<![\w])\d{1,4}[a-z]?(?![\w])',block['text']):
                        target=targets.get(match[0])
                        if not re.search(r',\s*$',block['text'][start:match.start()]):
                            continue
                        while start<match.start() and block['text'][start] in ' \u2192\uf0aa;':start+=1
                        if target:
                            links.append(dict(start=start,end=match.end(),topic_id=target['id'],key=target['topic_key']))
                        start=match.end()
                    block['chain_links']=links
                return jsonify(ok=True,topic=dict(id=row['id'],key=row['topic_key'],title=row['title'],
                    blocks=blocks,source_page=row['source_page']))
        except StorageUnavailable:
            return jsonify(ok=False,error='Bible Study data is unavailable.'),503

    @blueprint.get('/api/concordance')
    def concordance_api():
        query=_text(request.args.get('q',''))
        translation = requested_translation()
        try:
            offset=int(request.args.get('offset','0'))
        except ValueError:
            return jsonify(ok=False,error='Invalid concordance page.'),400
        if not query or translation is None or len(query)>100 or not 0<=offset<=10000:
            return jsonify(ok=False,error='Enter a word of up to 100 characters.'),400
        try:
            with database() as conn:
                if translation != 'NIV':
                    if conn.execute('PRAGMA user_version').fetchone()[0]<7:
                        return jsonify(ok=True,query=query,available=False,entries=[],next_offset=None)
                    term = unicodedata.normalize('NFKC',query).casefold()
                    rows=conn.execute('''SELECT DISTINCT v.id,v.book_id,v.chapter,v.verse,v.verse_end,v.text
                        FROM bible_tokens t JOIN bible_verses v ON v.id=t.verse_id
                        JOIN bible_editions e ON e.id=v.edition_id JOIN bible_import_runs r ON r.id=e.import_run_id
                        WHERE t.normalized_text=? AND e.translation=? AND e.is_active=1 AND v.is_active=1
                          AND e.status='ready' AND r.status='validated'
                        ORDER BY v.book_id,v.chapter,v.verse LIMIT 21 OFFSET ?''',(term,translation,offset)).fetchall()
                    present=conn.execute("SELECT 1 FROM bible_editions WHERE translation=? AND is_active=1 AND status='ready'",(translation,)).fetchone()
                    entries=[]
                    for row in rows[:20]:
                        reference=f"{BOOKS[row['book_id']-1][0]} {row['chapter']}:{row['verse']}"
                        if row['verse_end']>row['verse']: reference+='–'+str(row['verse_end'])
                        entries.append(dict(headword=reference,source_label=READER_TRANSLATIONS[translation]['label']+' · Word matches',
                            passage=dict(book=BOOKS[row['book_id']-1][0],chapter=row['chapter'],verse=row['verse']),
                            blocks=[dict(type='paragraph',text=reference,references=scripture_references(reference)),
                                    dict(type='paragraph',text=row['text'],references=[])],related=[]))
                    return jsonify(ok=True,query=query,translation=translation,available=bool(present),entries=entries,
                                   next_offset=offset+20 if len(rows)>20 else None)
                if conn.execute('PRAGMA user_version').fetchone()[0]<5:
                    return jsonify(ok=True,query=query,available=False,entries=[],next_offset=None)
                edition=conn.execute('''SELECT e.id FROM bible_editions e JOIN bible_import_runs r ON r.id=e.import_run_id
                    JOIN bible_edition_details d ON d.edition_id=e.id WHERE e.is_active=1 AND e.status='ready'
                    AND r.status='validated' AND d.source_kind='thompson_pdf' ''').fetchone()
                if not edition or not conn.execute('SELECT 1 FROM bible_thompson_concordance WHERE edition_id=? LIMIT 1',(edition[0],)).fetchone():
                    return jsonify(ok=True,query=query,available=False,entries=[],next_offset=None)
                term=normalize_term(query)
                exact=conn.execute('SELECT id FROM bible_thompson_concordance WHERE edition_id=? AND normalized_term=?',(edition[0],term)).fetchone()
                # Exact printed headwords take precedence. Cross-referenced word
                # forms are used only when the source has no exact heading.
                if exact:
                    rows=conn.execute('SELECT * FROM bible_thompson_concordance WHERE id=?',(exact[0],)).fetchall() if offset==0 else []
                else:
                    rows=conn.execute('''SELECT c.* FROM bible_thompson_concordance c WHERE c.edition_id=?
                        AND EXISTS(SELECT 1 FROM json_each(c.related_json) WHERE lower(value)=?)
                        ORDER BY c.normalized_term LIMIT 11 OFFSET ?''',(edition[0],term,offset)).fetchall()
                entries=[]
                for row in rows[:10]:
                    related=[]
                    for word in json.loads(row['related_json']):
                        if conn.execute('SELECT 1 FROM bible_thompson_concordance WHERE edition_id=? AND normalized_term=?',
                                        (edition[0],normalize_term(word))).fetchone():
                            related.append(word)
                    entries.append(dict(headword=row['headword'],blocks=json.loads(row['content_json']),
                                        related=related,source_page=row['source_page']))
                return jsonify(ok=True,query=query,available=True,entries=entries,next_offset=offset+10 if len(rows)>10 else None)
        except StorageUnavailable:
            return jsonify(ok=False,error='Bible Study data is unavailable.'),503

    @blueprint.get('/api/history')
    def history_get():
        try:
            with database() as conn:
                if conn.execute('PRAGMA user_version').fetchone()[0]<4:
                    return jsonify(ok=True,history=[])
                rows=conn.execute('''SELECT h.id,h.visited_at,b.name AS book,v.chapter,v.verse,e.translation,
                    substr(v.text,1,160) AS snippet,e.title AS edition FROM bible_navigation_history h
                    JOIN bible_verses v ON v.id=h.verse_id JOIN bible_books b ON b.id=v.book_id
                    JOIN bible_editions e ON e.id=v.edition_id WHERE h.user_key=? ORDER BY h.id DESC LIMIT 100''',(_account_key(),)).fetchall()
                return jsonify(ok=True,history=[dict(r) for r in rows])
        except StorageUnavailable:return jsonify(ok=True,history=[])

    @blueprint.post('/api/history')
    def history_save():
        data=request.get_json(silent=True)
        if not isinstance(data,dict) or set(data)!={'verse_id'} or type(data['verse_id']) is not int:
            return jsonify(ok=False,error='Invalid reading history.'),400
        try:
            with database(write=True) as conn:
                if conn.execute('PRAGMA user_version').fetchone()[0]<4:
                    return jsonify(ok=True,saved=False)
                row=conn.execute('''SELECT v.id,v.book_id,v.chapter,v.edition_id FROM bible_verses v JOIN bible_editions e ON e.id=v.edition_id
                    JOIN bible_import_runs r ON r.id=e.import_run_id WHERE v.id=? AND e.translation IN ('NIV','CEBOCB','TCB')
                    AND e.is_active=1 AND e.status='ready' AND r.status='validated' AND v.is_active=1''',(data['verse_id'],)).fetchone()
                if not row:return jsonify(ok=False,error='This passage is unavailable.'),404
                conn.execute('BEGIN IMMEDIATE')
                previous=conn.execute('''SELECT v.book_id,v.chapter,v.edition_id FROM bible_navigation_history h JOIN bible_verses v ON v.id=h.verse_id
                    WHERE h.user_key=? ORDER BY h.id DESC LIMIT 1''',(_account_key(),)).fetchone()
                if not previous or tuple(previous)!=(row['book_id'],row['chapter'],row['edition_id']):
                    conn.execute('INSERT INTO bible_navigation_history(user_key,verse_id,visited_at) VALUES(?,?,?)',
                        (_account_key(),row['id'],datetime.now(timezone.utc).isoformat()))
                    conn.execute('''DELETE FROM bible_navigation_history WHERE user_key=? AND id NOT IN
                        (SELECT id FROM bible_navigation_history WHERE user_key=? ORDER BY id DESC LIMIT 100)''',(_account_key(),_account_key()))
                conn.commit()
                return jsonify(ok=True,saved=True)
        except StorageUnavailable:return jsonify(ok=False,error='Reading history could not be saved.'),503

    @blueprint.get('/api/personal')
    def personal_api():
        book = request.args.get('book')
        book = BOOK_BY_NAME.get(book.casefold()) if book else None
        if request.args.get('book') and not book:
            return jsonify(ok=False, error='Choose a valid Bible book.'), 400
        chapter = request.args.get('chapter', type=int)
        if 'chapter' in request.args and (not book or chapter is None or not 1 <= chapter <= book[2]):
            return jsonify(ok=False, error='Choose a valid chapter.'), 400
        try:
            with database() as conn:
                if requested_translation() is None:
                    return jsonify(ok=False,error='Choose a supported translation.'),400
                items = _personal_bible_items(conn, _account_key(), book[0] if book else None, chapter,
                                              requested_translation() if book else None)
                study = _study_saved_items(conn, _account_key())
            return jsonify(ok=True, study_annotations=study['annotations'], study_bookmarks=study['bookmarks'], **items)
        except StorageUnavailable:
            return jsonify(ok=False, error='Saved Bible items are temporarily unavailable.'), 503

    @blueprint.post('/api/study-saves')
    def study_save_create():
        data = request.get_json(silent=True)
        if not isinstance(data,dict) or set(data)-{'kind','color','note','ranges'}:
            return jsonify(ok=False,error='Invalid study annotation.'),400
        kind,note,spans,color = data.get('kind'),data.get('note',''),data.get('ranges'),data.get('color')
        if (kind not in ('highlight','underline','note','bookmark') or not isinstance(note,str) or len(note)>8000
                or (kind=='note' and not note.strip()) or not isinstance(spans,list) or not 1<=len(spans)<=100
                or (kind=='bookmark' and len(spans)!=1)
                or not isinstance(color,(str,type(None))) or color not in (None,'#ffe66d','#9ee6b8','#9ed3ff','#ffd2df','#e5962d')):
            return jsonify(ok=False,error='Choose a valid annotation and selected text.'),400
        checked=[]
        try:
            signer=URLSafeSerializer(app.secret_key,salt='bible-study-text-v1')
            for span in spans:
                if (not isinstance(span,dict) or set(span)!={'token','text','start_offset','end_offset','selected_text'}
                        or not isinstance(span['token'],str) or not isinstance(span['text'],str)
                        or not isinstance(span['selected_text'],str) or len(span['text'])>100000
                        or type(span['start_offset']) is not int or type(span['end_offset']) is not int):
                    raise InvalidBibleSelection('Invalid study text anchor.')
                source=signer.loads(span['token'])
                if (not isinstance(source,dict) or source.get('digest')!=hashlib.sha256(span['text'].encode()).hexdigest()
                        or not 0<=span['start_offset']<span['end_offset']<=len(span['text'])
                        or span['text'][span['start_offset']:span['end_offset']]!=span['selected_text']):
                    raise InvalidBibleSelection('The selected source text changed. Select it again.')
                checked.append((span,source))
            with database(write=True) as conn:
                conn.execute('BEGIN IMMEDIATE')
                group,now=secrets.token_hex(16),datetime.now(timezone.utc).isoformat()
                for ordinal,(span,source) in enumerate(checked):
                    if kind=='bookmark':
                        existing=conn.execute("SELECT group_id FROM bible_study_saves WHERE user_key=? AND source_key=? AND kind='bookmark'",(_account_key(),source['key'])).fetchone()
                        if existing: group=existing['group_id'];continue
                    conn.execute('''INSERT INTO bible_study_saves
                        (id,user_key,group_id,kind,source_key,source_json,block_text,start_offset,end_offset,selected_text,color,note,created_at,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(group+'-'+str(ordinal).zfill(3),_account_key(),group,kind,source['key'],json.dumps(source),
                        span['text'],span['start_offset'],span['end_offset'],span['selected_text'],color,note.strip(),now,now))
                conn.commit()
                key='bookmarks' if kind=='bookmark' else 'annotations'
                item=next(item for item in _study_saved_items(conn,_account_key())[key] if item['group_id']==group)
            return jsonify(ok=True,item=item),201
        except (BadSignature,InvalidBibleSelection,KeyError,TypeError) as error:
            return jsonify(ok=False,error=str(error) if isinstance(error,InvalidBibleSelection) else 'Invalid study text anchor.'),400
        except StorageUnavailable:
            return jsonify(ok=False,error='Study saves are unavailable. A developer must initialize study-save storage before saving.'),503

    @blueprint.route('/api/study-saves/<item_id>',methods=['GET','PATCH','DELETE'])
    def study_saved_item_api(item_id):
        try:
            with database(write=request.method!='GET') as conn:
                if request.method!='GET':conn.execute('BEGIN IMMEDIATE')
                if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='bible_study_saves'").fetchone():
                    return jsonify(ok=False,error='Saved item not found.'),404
                row=conn.execute('SELECT * FROM bible_study_saves WHERE id=? AND user_key=?',(item_id,_account_key())).fetchone()
                if not row:return jsonify(ok=False,error='Saved item not found.'),404
                if request.method=='GET':
                    items=_study_saved_items(conn,_account_key())
                    item=next(item for item in items['annotations']+items['bookmarks'] if item['group_id']==row['group_id'])
                    return jsonify(ok=True,item=item)
                if request.method=='DELETE':
                    conn.execute('DELETE FROM bible_study_saves WHERE user_key=? AND group_id=?',(_account_key(),row['group_id']))
                else:
                    data=request.get_json(silent=True)
                    if (not isinstance(data,dict) or set(data)!={'note','version'} or not isinstance(data['note'],str)
                            or len(data['note'])>8000 or type(data['version']) is not int or (row['kind']=='note' and not data['note'].strip())):
                        return jsonify(ok=False,error='Enter a valid note.'),400
                    if data['version']!=row['version']:
                        return jsonify(ok=False,error='This note changed in another tab. Reopen it.'),409
                    conn.execute('''UPDATE bible_study_saves SET note=?,version=version+1,updated_at=? WHERE user_key=? AND group_id=?''',
                        (data['note'].strip(),datetime.now(timezone.utc).isoformat(),_account_key(),row['group_id']))
                conn.commit()
            return jsonify(ok=True)
        except StorageUnavailable:
            return jsonify(ok=False,error='Saved study items are temporarily unavailable.'),503

    def personal_verse(conn, verse_id):
        if type(verse_id) is not int:
            raise InvalidBibleSelection('Choose a valid verse.')
        row = conn.execute('''SELECT v.id,v.text,v.book_id,v.chapter,v.verse,v.edition_id
            FROM bible_verses v JOIN bible_editions e ON e.id=v.edition_id
            JOIN bible_import_runs r ON r.id=e.import_run_id WHERE v.id=? AND v.is_active=1
            AND e.translation IN ('NIV','CEBOCB','TCB') AND e.is_active=1 AND e.status='ready' AND r.status='validated' ''', (verse_id,)).fetchone()
        if row is None:
            raise InvalidBibleSelection('This Bible edition or verse is unavailable. Reload the chapter.')
        return row

    @blueprint.post('/api/annotations')
    def annotation_create():
        data = request.get_json(silent=True)
        if not isinstance(data, dict) or set(data) - {'kind', 'color', 'note', 'ranges'}:
            return jsonify(ok=False, error='Invalid annotation.'), 400
        kind, note, ranges = data.get('kind'), data.get('note', ''), data.get('ranges')
        colors = {'#ffe66d', '#9ee6b8', '#9ed3ff', '#ffd2df', '#e5962d'}
        if (kind not in ('highlight', 'underline', 'note') or not isinstance(note, str) or len(note) > 8000
                or not isinstance(ranges, list) or not 1 <= len(ranges) <= 100
                or not isinstance(data.get('color'), (str, type(None)))
                or data.get('color') not in colors | {None} or (kind == 'note' and not note.strip())):
            return jsonify(ok=False, error='Choose text, an annotation type, and a valid note or color.'), 400
        try:
            with database(write=True) as conn:
                conn.execute('BEGIN IMMEDIATE')
                previous = None
                checked = []
                for span in ranges:
                    if (not isinstance(span, dict) or set(span) != {'verse_id', 'start_offset', 'end_offset', 'selected_text'}
                            or any(type(span.get(key)) is not int for key in ('verse_id', 'start_offset', 'end_offset'))):
                        raise InvalidBibleSelection('Invalid selected text anchor.')
                    verse = personal_verse(conn, span['verse_id'])
                    start, end = span['start_offset'], span['end_offset']
                    if not (0 <= start < end <= len(verse['text'])) or verse['text'][start:end] != span['selected_text']:
                        raise InvalidBibleSelection('The selected text changed. Reload the chapter and select it again.')
                    position = (verse['edition_id'], verse['book_id'], verse['chapter'], verse['verse'])
                    if previous and (position[0] != previous[0] or position <= previous):
                        raise InvalidBibleSelection('Selected verses must be in order within one edition.')
                    previous = position
                    checked.append(span)
                group, now = secrets.token_hex(16), datetime.now(timezone.utc).isoformat()
                for span in checked:
                    item_id = secrets.token_hex(16)
                    conn.execute('''INSERT INTO bible_annotations
                        (id,user_key,verse_id,group_id,kind,start_offset,end_offset,selected_text,color,note,created_at,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', (item_id, _account_key(), span['verse_id'], group, kind,
                        span['start_offset'], span['end_offset'], span['selected_text'], data.get('color'), note.strip(), now, now))
                conn.commit()
                item = next(item for item in _personal_bible_items(conn, _account_key())['annotations'] if item['group_id'] == group)
            return jsonify(ok=True, annotation=item), 201
        except InvalidBibleSelection as error:
            return jsonify(ok=False, error=str(error)), 400
        except StorageUnavailable:
            return jsonify(ok=False, error='The annotation could not be saved. Please retry.'), 503

    @blueprint.post('/api/bookmarks')
    def bookmark_create():
        data = request.get_json(silent=True)
        if (not isinstance(data, dict) or set(data) - {'verse_id', 'label'}
                or not isinstance(data.get('label', ''), str) or len(data.get('label', '')) > 200):
            return jsonify(ok=False, error='Invalid bookmark.'), 400
        try:
            with database(write=True) as conn:
                conn.execute('BEGIN IMMEDIATE')
                verse = personal_verse(conn, data.get('verse_id'))
                item_id, now = secrets.token_hex(16), datetime.now(timezone.utc).isoformat()
                conn.execute('''INSERT INTO bible_bookmarks(id,user_key,verse_id,label,created_at,updated_at)
                    VALUES(?,?,?,?,?,?) ON CONFLICT(user_key,verse_id,character_offset)
                    DO UPDATE SET label=excluded.label,updated_at=excluded.updated_at''',
                    (item_id, _account_key(), verse['id'], data.get('label', '').strip(), now, now))
                conn.commit()
                item = next(item for item in _personal_bible_items(conn, _account_key())['bookmarks'] if item['verse_id'] == verse['id'])
            return jsonify(ok=True, bookmark=item), 201
        except InvalidBibleSelection as error:
            return jsonify(ok=False, error=str(error)), 400
        except StorageUnavailable:
            return jsonify(ok=False, error='The bookmark could not be saved. Please retry.'), 503

    @blueprint.route('/api/personal/<kind>/<item_id>', methods=['GET', 'PATCH', 'DELETE'])
    def personal_item_api(kind, item_id):
        if kind not in ('annotations', 'bookmarks') or not re.fullmatch(r'[\w-]{1,80}', item_id):
            return jsonify(ok=False, error='Saved item not found.'), 404
        try:
            with database(write=request.method != 'GET') as conn:
                table = 'bible_' + kind
                if request.method != 'GET':
                    conn.execute('BEGIN IMMEDIATE')
                row = conn.execute('SELECT * FROM ' + table + ' WHERE id=? AND user_key=?', (item_id, _account_key())).fetchone()
                if row is None:
                    return jsonify(ok=False, error='Saved item not found.'), 404
                if request.method == 'GET':
                    items = _personal_bible_items(conn, _account_key())[kind]
                    item = next(item for item in items if item.get('group_id', item['id']) == (row['group_id'] if kind == 'annotations' else row['id']))
                    return jsonify(ok=True, item=item)
                column, value = ('group_id', row['group_id']) if kind == 'annotations' else ('id', row['id'])
                if request.method == 'DELETE':
                    conn.execute('DELETE FROM ' + table + ' WHERE user_key=? AND ' + column + '=?', (_account_key(), value))
                else:
                    data = request.get_json(silent=True)
                    if (kind != 'annotations' or not isinstance(data, dict) or set(data) != {'note', 'version'}
                            or not isinstance(data['note'], str) or len(data['note']) > 8000 or type(data['version']) is not int
                            or (row['kind'] == 'note' and not data['note'].strip())):
                        return jsonify(ok=False, error='Enter a valid note.'), 400
                    if data['version'] != row['version']:
                        return jsonify(ok=False, error='This note changed in another tab. Reopen it before editing.'), 409
                    conn.execute('''UPDATE bible_annotations SET note=?,version=version+1,updated_at=?
                        WHERE user_key=? AND group_id=?''', (data['note'].strip(), datetime.now(timezone.utc).isoformat(), _account_key(), value))
                conn.commit()
            return jsonify(ok=True)
        except StorageUnavailable:
            return jsonify(ok=False, error='Saved Bible items are temporarily unavailable.'), 503

    @blueprint.get('/api/state')
    def state_get():
        try:
            with database() as conn:
                row = conn.execute('''SELECT b.name AS book,s.chapter,s.verse,s.theme,s.font_size,
                    s.layout_mode,s.split_ratio,s.version,COALESCE(e.translation,'NIV') AS translation FROM bible_reader_state s
                    JOIN bible_books b ON b.id=s.book_id LEFT JOIN bible_editions e ON e.id=s.edition_id WHERE s.user_key=?''', (_account_key(),)).fetchone()
            return jsonify(ok=True, state=dict(row) if row else default_state(), storage_available=True)
        except StorageUnavailable:
            return jsonify(ok=True, state=default_state(), storage_available=False)

    @blueprint.post('/api/state')
    def state_save():
        data = request.get_json(silent=True)
        allowed = {'book', 'chapter', 'verse', 'translation', 'theme', 'font_size', 'layout_mode', 'split_ratio', 'version'}
        if not isinstance(data, dict) or set(data) - allowed:
            return jsonify(ok=False, error='Invalid reader settings.'), 400
        book = BOOK_BY_NAME.get(str(data.get('book', '')).strip().casefold())
        numeric = ('chapter', 'verse', 'font_size', 'split_ratio', 'version')
        translation = data.get('translation','NIV')
        if not book or translation not in READER_TRANSLATIONS or any(type(data.get(key)) is not int for key in numeric):
            return jsonify(ok=False, error='Invalid reader settings.'), 400
        if not (1 <= data['chapter'] <= book[2] and 1 <= data['verse'] <= 176
                and 14 <= data['font_size'] <= 30 and 20 <= data['split_ratio'] <= 80 and data['version'] >= 0
                and data.get('theme') in ('paper', 'light', 'dark') and data.get('layout_mode') in ('split', 'bible', 'study')):
            return jsonify(ok=False, error='Invalid reader settings.'), 400
        try:
            with database(write=True) as conn:
                conn.execute('BEGIN IMMEDIATE')
                row = conn.execute('SELECT version FROM bible_reader_state WHERE user_key=?', (_account_key(),)).fetchone()
                current_version = row['version'] if row else 0
                if current_version != data['version']:
                    return jsonify(ok=False, error='Reader settings changed in another tab. Reload before saving.'), 409
                version = current_version + 1
                edition=conn.execute("SELECT id FROM bible_editions WHERE translation=? AND is_active=1 AND status='ready'",(translation,)).fetchone()
                if not edition and translation != 'NIV':
                    return jsonify(ok=False,error='This translation has not been imported yet.'),400
                conn.execute('''INSERT INTO bible_reader_state
                    (user_key,book_id,chapter,verse,theme,font_size,layout_mode,split_ratio,version,updated_at,edition_id)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(user_key) DO UPDATE SET
                    book_id=excluded.book_id,chapter=excluded.chapter,verse=excluded.verse,
                    theme=excluded.theme,font_size=excluded.font_size,layout_mode=excluded.layout_mode,
                    split_ratio=excluded.split_ratio,version=excluded.version,updated_at=excluded.updated_at,edition_id=excluded.edition_id''',
                    (_account_key(), book[0], data['chapter'], data['verse'], data['theme'], data['font_size'],
                     data['layout_mode'], data['split_ratio'], version, datetime.now(timezone.utc).isoformat(),edition[0] if edition else None))
                conn.commit()
            return jsonify(ok=True, version=version)
        except StorageUnavailable:
            return jsonify(ok=False, error='Bible Study storage is unavailable. Settings have not been saved.'), 503

    app.register_blueprint(blueprint)
    app.context_processor(lambda: {'bible_study_available': available})
    # Explicit operator entry point, intentionally never called at registration.
    app.bible_study_init_schema = lambda: initialize_database(configured_path(), static_folder=app.static_folder)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bible Study maintenance; does not import or start app.py.')
    parser.add_argument('command', choices=['init-db', 'init-study-saves', 'prepare-readers', 'import-step', 'sync-drive', 'sync-translations', 'format-thompson', 'import-concordance', 'import-studies'])
    parser.add_argument('--database', required=True, help='Absolute path to a separate Bible Study .db file.')
    parser.add_argument('--archive', help='Local official STEPBible-Data ZIP, required for import-step.')
    parser.add_argument('--ai-database', help='Existing private AI index, required for prepare-readers.')
    parser.add_argument('--pdf', help='Matching local Thompson PDF, required for format-thompson, import-concordance or import-studies.')
    parser.add_argument('--folder-id', default='', help='Official Bible Study Drive folder ID; otherwise use saved/discovered folder.')
    parser.add_argument('--translation-folder-id', default=resources.BIBLE_TRANSLATIONS_DRIVE_FOLDER_ID,
                        help='Separate reader-only Cebuano/Tagalog USFM ZIP folder; never used by the AI index.')
    parser.add_argument('--backup-directory', help='Private folder for the SQLite recovery backup.')
    parser.add_argument('--dry-run', action='store_true', help='Validate without applying changes to the live database.')
    arguments = parser.parse_args()
    try:
        if arguments.command=='prepare-readers':
            if not arguments.ai_database:parser.error('--ai-database is required for prepare-readers.')
            if arguments.dry_run:parser.error('prepare-readers writes only disposable private derived caches; omit --dry-run.')
            print(json.dumps(prepare_reader_catalogues(arguments.ai_database),indent=2))
        elif arguments.command=='init-study-saves':
            if arguments.dry_run:parser.error('init-study-saves creates a recovery backup and applies the additive upgrade; omit --dry-run')
            print(json.dumps(initialize_study_saves(arguments.database,arguments.backup_directory),indent=2))
        elif arguments.command in ('format-thompson','import-concordance','import-studies'):
            if not arguments.pdf:
                parser.error('--pdf is required for '+arguments.command)
            action=format_thompson_index if arguments.command=='format-thompson' else import_thompson_concordance
            options=dict(backup_directory=arguments.backup_directory, dry_run=arguments.dry_run)
            if arguments.command=='import-studies':options['include_studies']=True
            print(json.dumps(action(arguments.database, arguments.pdf, **options), indent=2))
        elif arguments.command == 'import-step':
            if not arguments.archive:
                parser.error('--archive is required for import-step')
            print(json.dumps(import_step_archive(arguments.database, arguments.archive,
                backup_directory=arguments.backup_directory, dry_run=arguments.dry_run), indent=2))
        elif arguments.command in ('sync-drive','sync-translations'):
            if arguments.dry_run:
                parser.error('--dry-run is supported for import-step; sync-drive always validates a temporary copy before applying')
            path=_database_path(arguments.database)
            lock=_sync_lock(path,create=True)
            if lock is None:raise SyncError('A Bible Study sync is already running.')
            try:
                progress=lambda message:print(message,flush=True)
                result=(sync_translation_sources(path,arguments.translation_folder_id,progress)
                        if arguments.command=='sync-translations' else
                        sync_sources(path,arguments.folder_id,progress,translation_folder_id=arguments.translation_folder_id))
                print(json.dumps(result,indent=2))
            finally:_release_sync_lock(lock)
        else:
            initialize_database(arguments.database)
            print('Bible Study schema initialized. No Bible content was imported.')
    except (ValueError, sqlite3.DatabaseError, OSError, SyncError, zipfile.BadZipFile) as error:
        parser.exit(1, f'Bible Study maintenance failed: {error}\n')
