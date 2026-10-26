"""Portability and incremental sync checks use temporary databases and fake Drive."""

import copy
import hashlib
import io
import json
import re
import zipfile
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask

import pastor_resources as resources
import pij_library_knowledge as knowledge


class LibraryPortabilityTests(unittest.TestCase):
    def test_reader_only_bibles_are_excluded_from_scan_index_and_cached_ai_search(self):
        keep = self.catalog()
        blocked = self.catalog('old-restricted', 843, 43, 'Synthetic restricted guide')
        self.document(keep, text='Synthetic Ginoo prayer evidence.')
        self.document(blocked, source_id=126, book_id=43, text='Synthetic Ginoo restricted evidence.')
        restricted_id = '1sYXbwGS2nT4MW9SUl1FcE7OvIebqtmzK'
        with closing(resources.get_resource_db()) as db, db:
            db.execute('UPDATE pastor_library_files SET drive_file_id=? WHERE id=843', (restricted_id,))
        with closing(knowledge._db()) as db, db:
            db.execute('UPDATE pij_library_documents SET drive_file_id=? WHERE source_file_id=126', (restricted_id,))
        self.assertEqual([r['book_id'] for r in knowledge._public_files_to_index()], [42])
        with closing(knowledge._db(read_only=True)) as db:
            self.assertEqual({r['book_id'] for r in knowledge._search_chunks(db, ['Ginoo'])}, {42})
        self.assertFalse(knowledge.get_library_book_status(43)['found'])
        self.assertEqual(knowledge.find_library_books('restricted')['count'], 0)
        blocked = dict(blocked,drive_file_id=restricted_id)
        with self.assertRaises(ValueError):
            knowledge._store_public_document(blocked, [(1,1,1,'Synthetic text')], 1)
        self.assertTrue(resources.ai_restricted_source({'name':'Biblica Open Tagalog Contemporary Bible 2025.zip'}))
        def listing(drive, folder):
            if folder == resources.PASTOR_RESOURCES_DRIVE_FOLDER_ID:
                return [dict(id=resources.BIBLE_TRANSLATIONS_DRIVE_FOLDER_ID, name='Restricted Bibles',
                             mimeType=resources.GOOGLE_DRIVE_FOLDER_MIME), self.drive_item()]
            raise AssertionError('The restricted folder must never be listed by library scanning.')
        with (patch.object(resources,'get_drive_session',return_value=object()),
              patch.object(resources,'list_drive_folder',side_effect=listing) as scanned,
              patch.object(resources,'get_title_and_author',return_value=('Prayer Guide','Author'))):
            resources.scan_drive_library()
        self.assertEqual(scanned.call_count,1)

    def test_study_folder_descendants_are_blocked_from_scan_store_and_cached_search(self):
        keep=self.catalog()
        blocked=self.catalog('study-file',843,43,'Synthetic study reference',fmt='EPUB')
        self.document(keep,text='Synthetic lighthouse evidence.')
        self.document(blocked,source_id=126,book_id=43,text='Synthetic lighthouse restricted evidence.')
        with closing(resources.get_resource_db()) as db,db:
            db.execute("UPDATE pastor_library_files SET folder_path='For Scripture Study App/BE Series' WHERE id=843")
            db.execute("UPDATE pastor_library_books SET folder_path='For Scripture Study App/BE Series' WHERE id=43")
        self.assertEqual([r['book_id'] for r in knowledge._public_files_to_index()],[42])
        with closing(knowledge._db(read_only=True)) as db:
            self.assertEqual({r['book_id'] for r in knowledge._search_chunks(db,['lighthouse'])},{42})
        blocked=dict(blocked,folder_path='For Scripture Study App/BE Series')
        with self.assertRaises(ValueError):knowledge._store_public_document(blocked,[(1,1,1,'text')],1)
        def listing(drive,folder):
            self.assertEqual(folder,resources.PASTOR_RESOURCES_DRIVE_FOLDER_ID)
            return [dict(id=resources.BIBLE_STUDY_DRIVE_FOLDER_ID,name='For Scripture Study App',mimeType=resources.GOOGLE_DRIVE_FOLDER_MIME)]
        with patch.object(resources,'get_drive_session',return_value=object()),patch.object(resources,'list_drive_folder',side_effect=listing) as scan:
            resources.scan_drive_library()
        self.assertEqual(scan.call_count,1)

    def test_study_epub_does_not_hide_a_pdf_of_the_same_book(self):
        pdf=self.catalog()
        epub=self.catalog('study-copy',843,42,'Prayer Guide',fmt='EPUB')
        self.document(pdf,text='Synthetic lighthouse evidence from PDF.')
        self.document(epub,source_id=126,book_id=42,text='Synthetic lighthouse excluded EPUB.')
        with closing(resources.get_resource_db()) as db,db:
            db.execute("UPDATE pastor_library_files SET folder_path='For Scripture Study App' WHERE id=843")
            db.execute("UPDATE pastor_library_books SET folder_path='For Scripture Study App' WHERE id=42")
        self.assertEqual([r['drive_file_id'] for r in knowledge._public_files_to_index()],['drive-A'])
        with closing(knowledge._db(read_only=True)) as db:
            matches=knowledge._search_chunks(db,['lighthouse'])
        self.assertEqual(len(matches),1)
        self.assertIn('from PDF',matches[0]['content'])

    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='district4_library_test_')
        self.addCleanup(directory.cleanup)
        self.app = SimpleNamespace(
            DATABASE=str(Path(directory.name) / 'app_v2.db'),
            AI_DATABASE=str(Path(directory.name) / 'ai_index.db'))
        for item in (patch.object(resources, '_appmod', return_value=self.app),
                     patch.object(knowledge, '_appmod', return_value=self.app),
                     patch.object(resources, 'RESOURCE_SYNC_STATE_DB',
                                  str(Path(directory.name) / 'sync.db')),
                     patch.object(resources, 'invalidate_thumbnail_cache')):
            item.start()
            self.addCleanup(item.stop)
        resources.ensure_v3_tables()
        knowledge.ensure_ai_library_tables()
        # Safety copies and the separate sermon subsystem must survive all jobs.
        db = sqlite3.connect(self.app.DATABASE)
        for table in ('pij_library_documents', 'pij_library_chunks',
                      'sermon_library_pages', 'sermon_library_pages_fts'):
            db.execute(f'CREATE TABLE {table} (marker TEXT)')
            db.execute(f"INSERT INTO {table} VALUES ('preserve')")
        db.commit()
        db.close()

    def tearDown(self):
        db = sqlite3.connect(self.app.DATABASE)
        try:
            for table in ('pij_library_documents', 'pij_library_chunks',
                          'sermon_library_pages', 'sermon_library_pages_fts'):
                self.assertEqual(db.execute(f'SELECT * FROM {table}').fetchall(), [('preserve',)])
        finally:
            db.close()

    def catalog(self, drive_id='drive-A', file_id=842, book_id=42, title='Prayer Guide',
                checksum='a' * 64, modified='2026-10-01T00:00:00Z', fmt='PDF'):
        db = resources.get_resource_db()
        try:
            key = resources.create_book_key(title, 'Author')
            db.execute('''INSERT OR IGNORE INTO pastor_library_books
                (id,book_key,title,author,category,folder_path,is_active,first_seen_at,last_seen_at)
                VALUES (?,?,?,'Author','Prayer','Prayer',1,'first','last')''', (book_id, key, title))
            db.execute('''INSERT INTO pastor_library_files
                (id,drive_file_id,book_id,name,format,mime_type,size,folder_path,created_time,
                 modified_time,md5_checksum,sha1_checksum,sha256_checksum,is_active,
                 first_seen_at,last_seen_at)
                VALUES (?,?,?,?,?,?,100,'Prayer','created',?,'','',?,1,'first','last')''',
                (file_id, drive_id, book_id, title + '.' + fmt.lower(), fmt,
                 'application/pdf' if fmt == 'PDF' else 'application/epub+zip', modified, checksum))
            db.commit()
        finally:
            db.close()
        return next(row for row in knowledge._public_files_to_index() if row['drive_file_id'] == drive_id)

    def document(self, row, source_id=125, book_id=7, text='Prayer strengthens faith and ministry.'):
        db = knowledge._db()
        try:
            cur = db.execute('''INSERT INTO pij_library_documents
                (source_type,source_file_id,drive_file_id,book_id,title,author,format,
                 modified_time,checksum,page_count,chunk_count,searchable,indexed_at)
                VALUES ('public_ebook',?,?,?,?,'Author',?,?,?,10,1,1,'original')''',
                (source_id, row['drive_file_id'], book_id, row['title'], row['format'],
                 row['modified_time'], row['checksum']))
            doc_id = cur.lastrowid
            db.execute('''INSERT INTO pij_library_chunks
                (document_id,chunk_number,page_start,page_end,content) VALUES (?,1,1,1,?)''', (doc_id, text))
            db.execute('''INSERT INTO pij_library_chunks_fts
                (document_id,chunk_number,content) VALUES (?,1,?)''', (doc_id, text))
            db.commit()
            return doc_id
        finally:
            db.close()

    def chunks(self):
        db = knowledge._db()
        try:
            return (db.execute('SELECT * FROM pij_library_chunks ORDER BY id').fetchall(),
                    db.execute('SELECT rowid,* FROM pij_library_chunks_fts ORDER BY rowid').fetchall())
        finally:
            db.close()

    def test_different_local_ids_reconcile_idempotently_and_preserve_chunks(self):
        row = self.catalog()
        doc_id = self.document(row)
        before = self.chunks()
        self.assertFalse(knowledge._needs_reindex(row))
        self.assertEqual(knowledge.reconcile_public_library_references(), 1)
        self.assertEqual(knowledge.reconcile_public_library_references(), 0)
        self.assertEqual(self.chunks(), before)
        db = knowledge._db()
        try:
            doc = db.execute('SELECT * FROM pij_library_documents WHERE id=?', (doc_id,)).fetchone()
            self.assertEqual((doc['source_file_id'], doc['book_id']), (842, 42))
            self.assertEqual(doc['indexed_at'], 'original')
        finally:
            db.close()

    def test_swapped_and_unmatched_local_id_collisions_preserve_all_text(self):
        a = self.catalog(file_id=10, book_id=10)
        b = self.catalog('drive-B', 20, 20, 'Second Guide')
        self.document(a, source_id=20, book_id=20)
        self.document(b, source_id=10, book_id=10)
        missing = dict(a, drive_file_id='missing-drive')
        self.document(missing, source_id=842, book_id=42)
        c = self.catalog('drive-C', 842, 42, 'New Guide')
        before = self.chunks()
        knowledge.reconcile_public_library_references()
        self.assertEqual(knowledge.reconcile_public_library_references(), 0)
        self.assertEqual(self.chunks(), before)
        db = knowledge._db()
        try:
            self.assertEqual(knowledge._find_public_document(db, 'drive-A')['source_file_id'], 10)
            self.assertEqual(knowledge._find_public_document(db, 'drive-B')['source_file_id'], 20)
            self.assertIsNone(knowledge._find_public_document(db, 'missing-drive')['book_id'])
        finally:
            db.close()
        # A new Drive file must not overwrite the unrelated document at its old local ID.
        knowledge._store_public_document(c, [(1, 1, 1, 'New guide content')], 1)
        self.assertEqual(len(self.chunks()[0]), 4)
        self.assertEqual(self.chunks()[0][:3], before[0])

    def test_unchanged_pass_never_authenticates_downloads_or_extracts(self):
        row = self.catalog()
        self.document(row)
        before = self.chunks()
        with (patch.object(resources, 'get_drive_session', side_effect=AssertionError('No auth needed')),
             patch.object(resources, 'download_drive_file_bytes', side_effect=AssertionError('No download')),
             patch.object(knowledge, '_extract_pdf', side_effect=AssertionError('No extraction'))):
            result = knowledge.index_public_library()
        self.assertEqual((result['checked'], result['skipped'], result['indexed'], result['errors']), (1, 1, 0, 0))
        self.assertEqual(self.chunks(), before)

    def test_only_modified_and_new_files_are_indexed(self):
        a = self.catalog()
        self.document(a)
        b = self.catalog('drive-B', 843, 43, 'Changed Guide')
        self.document(b, source_id=126)
        c = self.catalog('drive-C', 844, 44, 'New Guide')
        db = resources.get_resource_db()
        db.execute("UPDATE pastor_library_files SET sha256_checksum=?,modified_time='changed' WHERE drive_file_id='drive-B'", ('b' * 64,))
        db.commit()
        db.close()
        download = Mock(return_value=b'fake pdf')
        with (patch.object(resources, 'get_drive_session', return_value=object()),
             patch.object(resources, 'download_drive_file_bytes', download),
             patch.object(knowledge, '_extract_pdf', return_value=['Updated prayer and faith content'])):
            result = knowledge.index_public_library()
        self.assertEqual((result['skipped'], result['newly_indexed'], result['reindexed'], result['errors']), (1, 1, 1, 0))
        self.assertEqual({call.args[1] for call in download.call_args_list}, {'drive-B', 'drive-C'})
        self.assertEqual(len(self.chunks()[0]), 3)
        self.assertFalse(knowledge._needs_reindex(c))

    def test_fingerprint_priority_and_timestamp_fallback(self):
        row = self.catalog()
        self.document(row)
        self.assertFalse(knowledge._needs_reindex(dict(row, modified_time='metadata-only-change')))
        self.assertTrue(knowledge._needs_reindex(dict(row, checksum='b' * 64, sha256_checksum='b' * 64)))
        missing_hash = dict(row, checksum='', sha256_checksum='', md5_checksum='', sha1_checksum='')
        self.assertFalse(knowledge._needs_reindex(missing_hash))
        self.assertTrue(knowledge._needs_reindex(dict(missing_hash, modified_time='changed')))
        db = resources.get_resource_db()
        db.execute("UPDATE pastor_library_files SET sha256_checksum='',md5_checksum=?", ('d' * 32,))
        db.commit()
        db.close()
        refreshed = knowledge._public_files_to_index()[0]
        self.assertEqual(refreshed['checksum'], 'd' * 32)

    def test_uploaded_md5_index_is_reused_when_catalog_also_has_sha256(self):
        row = self.catalog()
        self.document(dict(row, checksum='d' * 32))
        self.assertFalse(knowledge._needs_reindex(dict(row, md5_checksum='d' * 32)))
        self.assertTrue(knowledge._needs_reindex(dict(row, md5_checksum='e' * 32)))

    def test_failed_reindex_preserves_last_successful_text_and_fingerprint(self):
        row = self.catalog()
        self.document(row)
        before = self.chunks()
        db = resources.get_resource_db()
        db.execute("UPDATE pastor_library_files SET sha256_checksum=?", ('b' * 64,))
        db.commit()
        db.close()
        with (patch.object(resources, 'get_drive_session', return_value=object()),
             patch.object(resources, 'download_drive_file_bytes', side_effect=RuntimeError('Temporary failure'))):
            self.assertEqual(knowledge.index_public_library()['errors'], 1)
        self.assertEqual(self.chunks(), before)
        self.assertTrue(knowledge._needs_reindex(knowledge._public_files_to_index()[0]))

    def test_database_details_counts_logical_books_using_drive_ids(self):
        a = self.catalog()
        self.document(a)
        # An unindexed EPUB of the same book must not inflate "Not AI Indexed".
        self.catalog('paired-epub', 843, 42, fmt='EPUB', checksum='b' * 64)
        self.catalog('new-drive', 844, 43, 'New Guide')
        details = resources.get_database_details_payload()
        self.assertEqual(details['summary']['logical_books'], 2)
        self.assertEqual(details['summary']['ai_indexed_books'], 1)
        self.assertEqual(details['summary']['ai_not_indexed_books'], 1)
        self.assertEqual(resources.get_database_details_payload(view='ai_indexed')['total'], 1)
        status = knowledge.get_library_book_status(42)
        self.assertTrue(status['indexed'])
        self.assertEqual(status['chunk_count'], 1)
        self.assertEqual(resources.search_library_database_v3('test-user', query='Prayer Guide')['books'][0]['page_count'], 10)

    def test_library_defaults_to_newest_with_stable_ties_and_title_sort_option(self):
        self.catalog(title='Alpha')
        self.catalog('newer', 843, 43, 'Zulu')
        self.catalog('same-date', 844, 44, 'Middle')
        db = resources.get_resource_db()
        db.execute("UPDATE pastor_library_books SET first_seen_at='2026-10-01T00:00:00Z'")
        db.execute("UPDATE pastor_library_books SET first_seen_at='2026-10-20T00:00:00Z' WHERE id IN (43,44)")
        db.commit()
        db.close()
        self.assertEqual([book['id'] for book in resources.search_library_database_v3('reader')['books']], [44, 43, 42])
        alphabetical = resources.search_library_database_v3('reader', mode='alphabetical')['books']
        self.assertEqual([book['title'] for book in alphabetical], ['Alpha', 'Middle', 'Zulu'])

    def test_manual_page_count_fills_missing_index_then_yields_to_index(self):
        row = self.catalog()
        resources.admin_edit_book(42, 'Prayer Guide', 'Author', 'Prayer', '250')
        def book():
            return resources.search_library_database_v3('reader')['books'][0]
        self.assertEqual((book()['page_count'], book()['manual_page_count']), (250, 250))
        self.document(row)
        self.assertEqual((book()['page_count'], book()['indexed_page_count'], book()['manual_page_count']), (10, 10, 250))
        # Metadata-only clients do not accidentally erase the fallback.
        resources.admin_edit_book(42, 'Renamed Guide', 'Author', 'Prayer')
        self.assertEqual(book()['manual_page_count'], 250)
        resources.admin_edit_book(42, 'Renamed Guide', 'Author', 'Prayer', '')
        self.assertIsNone(book()['manual_page_count'])
        self.assertEqual(book()['page_count'], 10)

    def test_invalid_manual_page_counts_do_not_partially_update_metadata(self):
        self.catalog()
        for invalid in ('0', '-1', '1.5', '1e3', 'pages', '1000001', True):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                resources.admin_edit_book(42, 'Wrong title', 'Wrong author', 'Other', invalid)
        book = resources.get_effective_book(42)
        self.assertEqual((book['title'], book['author']), ('Prayer Guide', 'Author'))
        self.assertIsNone(book['manual_page_count'])

    def test_recommendations_are_shared_ordered_and_separate_from_favorites(self):
        self.catalog()
        self.catalog('second', 843, 43, 'Another Guide')
        resources.set_book_favorite('reader-a', 42, True)
        with patch.object(resources, 'utc_now_iso', return_value='2026-10-19T00:00:00Z'):
            resources.admin_recommend_book(42, True)
        with patch.object(resources, 'utc_now_iso', return_value='2026-10-20T00:00:00Z'):
            resources.admin_recommend_book(43, True)
            resources.admin_recommend_book(42, True)
        for reader in ('reader-a', 'reader-b'):
            books = resources.search_library_database_v3(reader, mode='recommended')['books']
            self.assertEqual([book['id'] for book in books], [43, 42])
            self.assertTrue(all(book['recommended'] for book in books))
        self.assertEqual(resources.search_library_database_v3('reader-b', mode='favorites')['total'], 0)
        resources.admin_recommend_book(42, False)
        self.assertEqual(resources.search_library_database_v3('reader-a', mode='favorites')['total'], 1)
        self.assertEqual(resources.search_library_database_v3('reader-a', mode='recommended')['total'], 1)

    def test_unavailable_or_hidden_recommendations_are_excluded(self):
        self.catalog()
        resources.admin_recommend_book(42, True)
        for statement, restore in (
            ('UPDATE pastor_library_books SET is_hidden=1', 'UPDATE pastor_library_books SET is_hidden=0'),
            ('UPDATE pastor_library_books SET is_active=0', 'UPDATE pastor_library_books SET is_active=1'),
            ('UPDATE pastor_library_files SET is_active=0', 'UPDATE pastor_library_files SET is_active=1'),
        ):
            db = resources.get_resource_db()
            db.execute(statement)
            db.commit()
            db.close()
            self.assertEqual(resources.search_library_database_v3('reader', mode='recommended')['total'], 0)
            with self.assertRaises(ValueError):
                resources.admin_recommend_book(42, True)
            db = resources.get_resource_db()
            db.execute(restore)
            db.commit()
            db.close()
        self.assertEqual(resources.search_library_database_v3('reader', mode='recommended')['total'], 1)

    def test_manual_pages_and_recommendation_survive_sync_and_repeated_migration(self):
        self.catalog()
        resources.admin_edit_book(42, 'My title', 'My author', 'Prayer', 300)
        resources.admin_recommend_book(42, True)
        self.sync_drive([self.drive_item()])
        resources.ensure_v3_tables()
        resources.ensure_v3_tables()
        book = resources.search_library_database_v3('reader', mode='recommended')['books'][0]
        self.assertEqual((book['title'], book['author'], book['page_count'], book['manual_page_count']),
                         ('My title', 'My author', 300, 300))

    def test_developer_endpoints_authorize_validate_and_save_new_fields(self):
        self.catalog()
        app = Flask(__name__)
        app.secret_key = 'test-only'
        resources.register_pastor_resources_routes(app)
        client = app.test_client()
        recommend_url = '/pastor-resources/admin/recommend/42'
        edit_url = '/pastor-resources/admin/edit/42'
        for credentials in ({}, {'ao_logged_in': True, 'ao_username': 'other', 'ao_role': 'Area Overseer'}):
            with client.session_transaction() as session:
                session.clear()
                session.update(credentials)
            self.assertEqual(client.post(recommend_url, json={'recommended': True}).status_code, 403)
            self.assertEqual(client.post(edit_url, json={'title': 'X', 'manual_page_count': 20}).status_code, 403)
        with client.session_transaction() as session:
            session.update(ao_logged_in=True, ao_username='Pijeme', ao_role='Area Overseer')
        for invalid in ({}, {'recommended': 'false'}, {'recommended': 1}, ['bad']):
            self.assertEqual(client.post(recommend_url, json=invalid).status_code, 400)
        self.assertEqual(client.post(recommend_url, json={'recommended': True}).status_code, 200)
        response = client.post(edit_url, json={'title': 'Manual Guide', 'author': 'Author',
                                             'category': 'Prayer', 'manual_page_count': 200})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['book']['manual_page_count'], 200)
        with patch.object(resources, 'any_user_logged_in', return_value=True):
            public = client.get('/pastor-resources/api/books?mode=recommended')
        self.assertEqual(public.status_code, 200)
        self.assertEqual(public.json['books'][0]['page_count'], 200)

    def test_database_details_export_includes_all_pages_and_preserves_sort(self):
        for index in range(105):
            self.catalog(f'file-{index}', 1000 + index, 1000 + index,
                         f'Guide {index:03}', modified=f'{index:03}')
        page = resources.get_database_details_payload(view='logical', per_page=20)
        self.assertEqual((len(page['items']), page['total']), (20, 105))
        details = resources.get_database_details_payload(
            view='logical', sort='modified_desc', per_page=20, page=4, export=True)
        self.assertEqual(len(details['items']), 105)
        self.assertEqual(details['items'][0]['title'], 'Guide 104')
        self.assertEqual(details['items'][-1]['title'], 'Guide 000')

    def test_database_details_export_groups_all_current_formats_and_locations(self):
        self.catalog()
        self.catalog('epub', 843, 42, fmt='EPUB')
        self.catalog('old-file', 844, 42)
        self.catalog('another-book', 845, 43, 'Other Book')
        db = resources.get_resource_db()
        db.execute("UPDATE pastor_library_books SET manual_title='Edited Café', manual_author='José' WHERE id=42")
        db.execute("UPDATE pastor_library_files SET folder_path='Authors/PDF Shelf' WHERE id=842")
        db.execute("UPDATE pastor_library_files SET folder_path='Authors/EPUB Shelf' WHERE id=843")
        db.execute("UPDATE pastor_library_files SET is_active=0, folder_path='Old Folder' WHERE id=844")
        db.commit()
        db.close()
        # Only the EPUB path matches, but the selected book includes both formats.
        details = resources.get_database_details_payload(
            view='logical', query='EPUB Shelf', export=True)
        output = resources.format_database_details_txt(details)
        self.assertEqual(output.count('Book Name:'), 1)
        self.assertIn('Book Name: Edited Café\r\nAuthor: José', output)
        self.assertIn('File Type: EPUB, PDF', output)
        self.assertIn('EPUB: Authors/EPUB Shelf', output)
        self.assertIn('PDF: Authors/PDF Shelf', output)
        self.assertNotIn('Old Folder', output)
        self.assertNotIn('Other Book', output)
        self.assertNotIn('drive_file_id', output)

    def test_database_details_export_uses_same_category_membership_as_browser(self):
        indexed = self.catalog()
        self.document(indexed)
        self.catalog('epub', 843, 42, fmt='EPUB')
        self.catalog('duplicate', 844, 43, 'Duplicate Guide')
        self.catalog('hidden', 845, 44, 'Hidden Guide')
        self.catalog('inactive', 846, 45, 'Inactive Guide')
        db = resources.get_resource_db()
        db.execute("UPDATE pastor_library_files SET is_duplicate=1 WHERE id=844")
        db.execute("UPDATE pastor_library_books SET is_hidden=1 WHERE id=44")
        db.execute("UPDATE pastor_library_files SET is_active=0 WHERE id=846")
        db.commit()
        db.close()
        expected = {'all': 4, 'logical': 3, 'pdf': 3, 'epub': 1, 'exact': 1,
                    'same_name': 2, 'hidden': 1, 'inactive': 1,
                    'ai_indexed': 1, 'ai_not_indexed': 1}
        for view, count in expected.items():
            with self.subTest(view=view):
                browser = resources.get_database_details_payload(view=view)
                exported = resources.get_database_details_payload(view=view, export=True)
                self.assertEqual(browser['total'], count)
                self.assertEqual([item['database_file_id'] for item in exported['items']],
                                 [item['database_file_id'] for item in browser['items']])

    def test_database_details_export_handles_root_unknown_author_and_no_matches(self):
        self.catalog()
        db = resources.get_resource_db()
        db.execute("UPDATE pastor_library_books SET author='' ")
        db.execute("UPDATE pastor_library_files SET folder_path='' ")
        db.commit()
        db.close()
        output = resources.format_database_details_txt(
            resources.get_database_details_payload(view='pdf', export=True))
        self.assertIn('Author: Unknown Author', output)
        self.assertIn("Google Drive Folder: Pastor's Resources (root folder)", output)
        empty = resources.format_database_details_txt(
            resources.get_database_details_payload(query='no matching book', export=True))
        self.assertIn('No records found.', empty)
        self.assertNotIn('Book Name:', empty)

    def test_database_details_download_requires_actual_administrator_session(self):
        app = Flask(__name__)
        app.secret_key = 'test-only'
        resources.register_pastor_resources_routes(app)
        client = app.test_client()
        path = '/pastor-resources/admin/database-details/download'
        with patch.object(resources, 'get_database_details_payload') as read:
            for credentials in ({}, {'ao_logged_in': True, 'ao_username': 'someone',
                                     'ao_role': 'area overseer'},
                                {'ao_logged_in': True, 'ao_username': 'pijeme',
                                 'ao_role': 'pastor'}):
                with client.session_transaction() as session:
                    session.clear()
                    session.update(credentials)
                response = client.get(path)
                self.assertEqual(response.status_code, 403)
                self.assertNotIn('Content-Disposition', response.headers)
            read.assert_not_called()

    def test_database_details_download_returns_unicode_txt_attachment(self):
        self.catalog(title='Café Ministry')
        app = Flask(__name__)
        app.secret_key = 'test-only'
        resources.register_pastor_resources_routes(app)
        client = app.test_client()
        with client.session_transaction() as session:
            session.update(ao_logged_in=True, ao_username='Pijeme', ao_role='Area Overseer')
        with (patch.object(resources, 'get_drive_session', side_effect=AssertionError('No Drive access')),
              patch.object(resources, 'download_drive_file_bytes', side_effect=AssertionError('No file downloads'))):
            response = client.get('/pastor-resources/admin/database-details/download',
                                  query_string={'view': 'logical', 'q': 'Café'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, 'text/plain')
        self.assertEqual(response.content_type, 'text/plain; charset=utf-8')
        self.assertIn('attachment; filename=logical-books.txt', response.headers['Content-Disposition'])
        self.assertEqual(response.headers['Cache-Control'], 'private, no-store')
        self.assertIn('Book Name: Café Ministry', response.data.decode('utf-8-sig'))
        response.close()

    def test_database_details_download_template_resolves_endpoint(self):
        app = Flask(__name__)
        resources.register_pastor_resources_routes(app)
        with app.test_request_context():
            # Render the real template with a minimal base to avoid starting app.py.
            from jinja2 import DictLoader
            app.jinja_loader = DictLoader({'base.html': '{% block content %}{% endblock %}'})
            html = app.jinja_env.from_string(resources.PASTOR_DATABASE_DETAILS_HTML).render()
        self.assertIn('/pastor-resources/admin/database-details/download', html)
        self.assertIn('Download Details (.txt)', html)

    def test_search_and_catalog_work_before_id_reconciliation_and_respect_visibility(self):
        row = self.catalog()
        self.document(row)
        db = knowledge._db()
        try:
            hits = knowledge._search_chunks(db, ['prayer'], book_id=42)
            self.assertEqual(hits[0]['book_id'], 42)
            self.assertEqual(len(knowledge._opening_chunks_for_book(db, 42)), 1)
            self.assertEqual(knowledge._active_indexed_books(db)[0]['book_id'], 42)
        finally:
            db.close()
        self.assertTrue(knowledge.find_library_books('Prayer')['books'][0]['indexed'])
        self.assertTrue(knowledge._catalog_specific_book_for_question('Tell me about Prayer Guide')['indexed'])
        self.assertEqual(knowledge.search_library_index('prayer', book_id=42)['count'], 1)
        self.assertEqual(knowledge.get_index_state()['searchable_documents'], 1)
        db = resources.get_resource_db()
        db.execute('UPDATE pastor_library_books SET is_hidden=1')
        db.commit()
        db.close()
        self.assertEqual(knowledge.search_library_index('prayer')['count'], 0)
        self.assertEqual(knowledge.find_library_books('Prayer')['count'], 0)
        self.assertEqual(knowledge._public_files_to_index(), [])

    def test_search_fallback_and_inactive_or_duplicate_files(self):
        row = self.catalog()
        self.document(row)
        db = knowledge._db()
        try:
            with patch.object(knowledge, '_fts_available', return_value=False):
                self.assertEqual(knowledge._search_chunks(db, ['prayer'], book_id=42)[0]['book_id'], 42)
            db.execute('UPDATE appdb.pastor_library_files SET is_active=0')
            db.commit()
            self.assertEqual(knowledge._search_chunks(db, ['prayer']), [])
            db.execute('UPDATE appdb.pastor_library_files SET is_active=1,is_duplicate=1')
            db.commit()
            self.assertEqual(knowledge._search_chunks(db, ['prayer']), [])
        finally:
            db.close()

    def test_book_counts_respect_smaller_requests_and_cap_at_ten(self):
        for question, expected in (
            ('Suggest books about giving', 10),
            ('Suggest a book about giving', 1),
            ('Suggest one resource about giving', 1),
            ('Suggest 3 books about giving', 3),
            ('Suggest seven books about giving', 7),
            ('Suggest eight ebooks about giving', 8),
            ('Suggest nine resources about giving', 9),
            ('Suggest ten books about giving', 10),
            ('Suggest up to 10 books about giving', 10),
            ('Suggest 25 books about giving', 10),
        ):
            with self.subTest(question=question):
                self.assertEqual(knowledge._requested_book_count(question), expected)
        self.assertEqual(knowledge._requested_book_count('Suggest books', default=0), 0)
        self.assertEqual(knowledge._query_terms('List up to ten books about giving'), ['giving'])

    def test_reading_request_wording_does_not_become_a_recommendation_topic(self):
        self.document(self.catalog(title='Discipleship Guide'),
                      text='Discipleship means helping people become faithful followers of Jesus.')
        self.document(self.catalog('drive-reading', 843, 43, 'How to Read Books'), source_id=126,
                      text='Read a chapter slowly and take notes about its structure.')
        question = 'What books should I read about discipleship?'
        self.assertEqual(knowledge._query_terms(question), ['discipleship'])
        context = knowledge.library_context_for_gemini(question)
        self.assertIn('Discipleship Guide', context)
        self.assertNotIn('How to Read Books', context)
        self.assertIn('read', knowledge._query_terms('Tell me about "How to Read Books"'))
        self.assertIn('reading', knowledge._query_terms('Recommend books about reading Scripture'))

    def test_prayer_and_leadership_term_families_find_relevant_passages(self):
        self.document(self.catalog(title='Ministry Handbook'),
                      text='Praying helps us depend on God. A leader serves people with humility.')
        with closing(knowledge._db(read_only=True)) as db:
            self.assertEqual(len(knowledge._search_chunks(db, ['prayer'])), 1)
            self.assertEqual(len(knowledge._search_chunks(db, ['leadership'])), 1)
        self.assertIn('prayer', knowledge._term_variants('pray'))
        self.assertIn('leadership', knowledge._term_variants('leader'))

    def test_exact_book_openings_balance_formats_and_skip_duplicate_front_matter(self):
        pdf = self.catalog(title='Ministry Handbook')
        epub = self.catalog('drive-epub', 843, 42, 'Ministry Handbook', fmt='EPUB')
        duplicate = 'Introduction. This handbook helps ministry leaders serve their churches faithfully. ' * 4
        pdf_doc = self.document(pdf, text='Copyright 2020. All rights reserved. ISBN 1234.')
        epub_doc = self.document(epub, source_id=126, text=duplicate)
        with closing(knowledge._db()) as db, db:
            db.executemany('''INSERT INTO pij_library_chunks
                (document_id,chunk_number,page_start,page_end,content) VALUES (?,?,?,?,?)''', [
                (pdf_doc, 2, 2, 2, duplicate),
                (pdf_doc, 3, 3, 3, 'Preface. Practical guidance for leading small groups and supporting church volunteers.'),
                (epub_doc, 2, 2, 2, 'Contents. Serving the church. Developing leaders. Encouraging volunteers.'),
            ])
        with closing(knowledge._db(read_only=True)) as db:
            rows = knowledge._opening_chunks_for_book(db, 42, limit=3)
        self.assertEqual({row['format'].upper() for row in rows}, {'PDF', 'EPUB'})
        self.assertEqual(sum(row['content'] == duplicate for row in rows), 1)
        self.assertNotIn('Copyright', '\n'.join(row['content'] for row in rows))
        self.assertTrue(all(row['book_id'] == 42 for row in rows))

    def test_generic_book_followup_reads_intro_and_stays_on_the_named_book(self):
        self.document(self.catalog(title='Ministry Handbook'),
                      text='Introduction. This handbook offers guidance for caring for church volunteers.')
        self.document(self.catalog('drive-other', 843, 43, 'More Ministry Ideas'), source_id=126,
                      text='More ideas for unrelated activities.')
        context = knowledge.library_context_for_gemini('Tell me more about it', history=[
            {'role': 'user', 'content': 'Tell me about "Ministry Handbook"'}])
        self.assertIn('RETRIEVAL MODE: SPECIFIC_BOOK', context)
        self.assertIn('caring for church volunteers', context)
        self.assertNotIn('More Ministry Ideas', context)

    def test_excerpt_prefers_joint_topic_explanation_to_first_incidental_mention(self):
        passage = ('Prayer. ' + 'Unrelated background. ' * 90
                   + 'Prayer and spiritual warfare require dependence on God rather than personal strength. ' * 4)
        excerpt = knowledge._topic_excerpt(passage, 280, ['prayer', 'spiritual', 'warfare'])
        self.assertLessEqual(len(excerpt), 280)
        self.assertIn('dependence on God', excerpt)
        self.assertIn('spiritual warfare', excerpt)

    def test_ten_recommendations_keep_every_source_and_topic_within_context_budget(self):
        for number in range(12):
            row = self.catalog(f'drive-{number}', 100 + number, 100 + number,
                               f'Generosity Volume {number}')
            # The matching topic is deliberately late in a long passage.
            self.document(row, source_id=100 + number,
                          text='Opening material. ' * 220 + 'Giving generously helps others. ' * 60)
        for question, expected in (
            ('Suggest books about giving', 10),
            ('List up to ten books about giving', 10),
            ('Suggest 25 books about giving', 10),
            ('Suggest 3 books about giving', 3),
            ('Suggest a book about giving', 1),
        ):
            with self.subTest(question=question):
                context = knowledge.library_context_for_gemini(question)
                self.assertEqual(context.count('\nBOOK '), expected)
                self.assertEqual(context.count('APPROVED LINK:'), expected)
                self.assertEqual(context.count('PASSAGE:'), expected)
                self.assertLessEqual(len(context), knowledge.MAX_CONTEXT_CHARS)
                for block in context.split('\nBOOK ')[1:]:
                    self.assertIn('Giving', block)
                    self.assertRegex(block, r'APPROVED LINK: /pastor-resources/read/1\d\d\?')

    def test_recommendations_use_only_available_relevant_books(self):
        self.document(self.catalog(title='Generosity Guide'), text='Giving generously helps others.')
        self.document(self.catalog('drive-B', 843, 43, 'Unrelated Guide'), source_id=126,
                      text='Music practice improves rhythm.')
        context = knowledge.library_context_for_gemini('Suggest ten books about giving')
        self.assertEqual(context.count('\nBOOK '), 1)
        self.assertIn('Generosity Guide', context)
        self.assertNotIn('Unrelated Guide', context)
        self.assertIn('list fewer rather than padding', context)

    def test_preferred_sources_are_found_even_when_study_bible_fills_global_hits(self):
        bible = self.catalog(title='Alpha Study Bible')
        doc_id = self.document(bible, text='Giving generously helps.')
        devotional = self.catalog('drive-devotional', 843, 43, 'Daily Devotional Reflections')
        self.document(devotional, source_id=126,
                      text='Opening reflection. ' * 300 + 'Giving generously helps others.')
        commentary = self.catalog('drive-commentary', 844, 44, 'Ministry Commentary')
        self.document(commentary, source_id=127,
                      text='Context and explanation. ' * 300 + 'Giving generously helps others.')
        hidden = self.catalog('drive-hidden', 845, 45, 'Hidden Devotional')
        self.document(hidden, source_id=128, text='Giving generously helps.')
        with closing(knowledge._db()) as db:
            db.execute('UPDATE appdb.pastor_library_books SET is_hidden=1 WHERE id=45')
            for number in range(2, 162):
                db.execute('''INSERT INTO pij_library_chunks
                    (document_id,chunk_number,page_start,page_end,content) VALUES(?,?,?,?,?)''',
                           (doc_id, number, number, number, 'Giving generously helps.'))
                db.execute('INSERT INTO pij_library_chunks_fts(document_id,chunk_number,content) VALUES(?,?,?)',
                           (doc_id, number, 'Giving generously helps.'))
            db.execute('UPDATE pij_library_documents SET chunk_count=161 WHERE id=?', (doc_id,))
            db.commit()
            global_hits = knowledge._search_chunks(db, ['giving'], limit=140)
            self.assertEqual({row['book_id'] for row in global_hits}, {42})
            ranked, mode = knowledge._rank_books(db, 'Suggest books about giving', 'recommendation', 10)
            self.assertEqual(mode, 'recommendation')
            self.assertEqual({book['book_id'] for book in ranked[:2]}, {43, 44})
            self.assertEqual(ranked[-1]['book_id'], 42)
            self.assertNotIn(45, [book['book_id'] for book in ranked])
            # The subset filter also applies when FTS5 is unavailable.
            with patch.object(knowledge, '_fts_available', return_value=False):
                subset = knowledge._search_chunks(db, ['giving'], book_ids=[43, 44, 45])
            self.assertEqual({row['book_id'] for row in subset}, {43, 44})

        context = knowledge.library_context_for_gemini('Suggest a book about giving')
        self.assertNotIn('Alpha Study Bible', context)
        self.assertEqual(context.count('APPROVED LINK:'), 1)
        self.assertIn('present commentaries and devotional books first', context)
        exact = knowledge.library_context_for_gemini('Tell me about "Alpha Study Bible"')
        self.assertIn('BOOK 1: Alpha Study Bible', exact)
        self.assertNotIn('Daily Devotional Reflections', exact)

    def test_extra_matching_pages_do_not_increase_a_books_score(self):
        doc_id = self.document(self.catalog(title='Alpha Study Bible'), text='Giving generously helps.')
        with closing(knowledge._db()) as db:
            before, _ = knowledge._rank_books(db, 'Suggest books about giving', 'recommendation', 10)
            for number in range(2, 5):
                db.execute('''INSERT INTO pij_library_chunks
                    (document_id,chunk_number,page_start,page_end,content) VALUES(?,?,?,?,?)''',
                           (doc_id, number, number, number, 'Giving generously helps.'))
                db.execute('INSERT INTO pij_library_chunks_fts(document_id,chunk_number,content) VALUES(?,?,?)',
                           (doc_id, number, 'Giving generously helps.'))
            db.commit()
            after, _ = knowledge._rank_books(db, 'Suggest books about giving', 'recommendation', 10)
        self.assertEqual(before[0]['score'], after[0]['score'])

    def test_study_bible_classification_takes_precedence_over_commentary_category(self):
        self.assertEqual(knowledge._resource_priority({
            'title': 'Life Application Study Bible', 'category': 'Commentaries', 'folder_path': ''}), 2)
        self.assertEqual(knowledge._resource_priority({
            'title': 'NIV Life Journey Bible', 'category': 'Study Bibles', 'folder_path': ''}), 2)
        self.assertEqual(knowledge._resource_priority({
            'title': 'NIV Bible Study Commentary', 'category': 'General', 'folder_path': ''}), 0)
        self.assertEqual(knowledge._resource_priority({
            'title': 'Reflection Guide', 'category': 'Devotionals', 'folder_path': ''}), 0)
        self.assertFalse(knowledge._use_resource_priority('Suggest study Bibles about giving', 'recommendation'))

    def test_typed_counts_and_routes_accept_natural_resource_requests(self):
        for question, kind, count in (
            ('suggest 10 study bible?', 'study_bible', 10),
            ('show 3 study Bibles', 'study_bible', 3),
            ('suggest five devotional books', 'devotional', 5),
            ('list two commentaries', 'commentary', 2),
            ("suggest four children's ministry books", 'children_ministry', 4),
        ):
            with self.subTest(question=question):
                self.assertEqual(knowledge._library_mode(question), 'recommendation')
                self.assertEqual(knowledge._requested_book_count(question), count)
                self.assertEqual(knowledge._requested_resource_type(question), kind)
                self.assertEqual(knowledge._resource_topic_terms(question, kind), [])

    def test_study_bible_browse_uses_catalogue_without_passage_search(self):
        for number in range(12):
            # Intentionally not indexed: catalogue availability and full-text
            # searchability are separate and recommendations must respect that.
            self.catalog(f'drive-{number}', 100 + number, 100 + number,
                         f'Edition {number} Study Bible')
        with closing(resources.get_resource_db()) as db:
            db.execute("UPDATE pastor_library_books SET is_hidden=1 WHERE id=100")
            db.execute("UPDATE pastor_library_files SET is_active=0 WHERE book_id=101")
            db.execute("UPDATE pastor_library_files SET is_duplicate=1 WHERE book_id=102")
            db.commit()
        self.catalog('drive-commentary', 200, 200, 'Daily Study Bible')
        self.catalog('drive-handbook', 201, 201, 'Bible Study Handbook')
        with (patch.object(knowledge, '_search_chunks', side_effect=AssertionError('No passage search')),
              patch.object(knowledge, '_catalog_specific_book_for_question', side_effect=AssertionError('Not one title'))):
            context = knowledge.library_context_for_gemini('suggest 10 study bible?')
        self.assertEqual(context.count('\nBOOK '), 9)
        self.assertEqual(context.count('APPROVED SOURCE:'), 9)
        self.assertEqual(len(context.approved_sources), 9)
        self.assertTrue(all(source['reason']['English'] for source in context.approved_sources))
        self.assertNotIn('Daily Study Bible', context)
        self.assertNotIn('Bible Study Handbook', context)
        self.assertNotIn('Edition 0 Study Bible', context)
        self.assertIn('not extracted passages', context)
        self.assertIn('do not claim the whole library', context)
        self.assertLessEqual(len(context), knowledge.MAX_CONTEXT_CHARS)
        self.assertEqual(knowledge.library_context_for_gemini('suggest 3 study Bibles').count('\nBOOK '), 3)

    def test_children_ministry_browse_prioritizes_practical_books_over_incidental_commentaries(self):
        for number, title in enumerate(('Crafts for Children', 'Sunday School Lessons', 'Kids Devotions',
                                       'Talking with Your Kids', 'General Bible Commentary')):
            row = self.catalog(f'drive-{number}', 100 + number, 100 + number, title)
            self.document(row, source_id=100 + number, text='Children and ministry are mentioned here.')
        context = knowledge.library_context_for_gemini('suggest 10 books for children ministry')
        self.assertEqual(context.count('\nBOOK '), 4)
        self.assertIn('BOOK 1: Crafts for Children', context)
        self.assertLess(context.index('Sunday School Lessons'), context.index('Kids Devotions'))
        self.assertLess(context.index('Kids Devotions'), context.index('Talking with Your Kids'))
        self.assertNotIn('General Bible Commentary', context)
        self.assertIn('Bible-themed crafts', context.approved_sources[0]['reason']['English'])

    def test_topical_fit_comes_before_commentary_preference(self):
        self.document(self.catalog(title='Generosity Giving Guide'), text='Giving generously helps others.')
        self.document(self.catalog('drive-C', 843, 43, 'General Bible Commentary'), source_id=126,
                      text='Giving generously helps others.')
        with closing(knowledge._db()) as db:
            ranked, mode = knowledge._rank_books(db, 'Suggest books about giving', 'recommendation', 10)
        self.assertEqual(ranked[0]['title'], 'Generosity Giving Guide')

    def test_typed_topic_search_excludes_other_source_types(self):
        self.document(self.catalog(title='Prayer Study Bible'), text='Prayer helps believers.')
        self.document(self.catalog('drive-C', 843, 43, 'Prayer Commentary'), source_id=126,
                      text='Prayer helps believers.')
        context = knowledge.library_context_for_gemini('Suggest 3 study Bibles about prayer')
        self.assertIn('Prayer Study Bible', context)
        self.assertNotIn('Prayer Commentary', context)

    def test_loving_others_recommendations_do_not_collapse_to_a_similar_title(self):
        for number, title in enumerate(('Love One Another', 'Christian Compassion', 'Loving Our Neighbors',
                                       'Practical Christian Care', 'Helping People', 'Fellowship Guide')):
            row = self.catalog(f'drive-{number}', 100 + number, 100 + number, title)
            self.document(row, source_id=100 + number,
                          text='Love others by listening, showing compassion, and helping neighbors in practical ways.')
        self.assertIsNone(knowledge._catalog_specific_book_for_question('suggest 5 books about loving others?'))
        with closing(knowledge._db()) as db:
            self.assertIsNone(knowledge._find_specific_book(db, 'suggest books about loving others?', ['loving', 'others']))
        context = knowledge.library_context_for_gemini('suggest 5 books about loving others?')
        self.assertIn('RETRIEVAL MODE: RECOMMENDATION', context)
        self.assertEqual(context.count('\nBOOK '), 5)
        self.assertEqual(len({source['title'] for source in context.approved_sources}), 5)
        exact = knowledge.library_context_for_gemini('Tell me about the book "Love One Another"')
        self.assertIn('RETRIEVAL MODE: SPECIFIC_BOOK', exact)
        self.assertEqual(exact.count('\nBOOK '), 1)
        self.assertIn('BOOK 1: Love One Another', exact)

    def test_recommendations_do_not_repeat_titles_with_imported_author_differences(self):
        original = self.catalog(title='Love One Another')
        duplicate = self.catalog('drive-B', 843, 43, 'Temporary Imported Title')
        other = self.catalog('drive-C', 844, 44, 'Loving Our Neighbors')
        with closing(resources.get_resource_db()) as db:
            db.execute("UPDATE pastor_library_books SET title='Love One Another',author='Epub Files' WHERE id=43")
            db.commit()
        for number, row in enumerate((original, duplicate, other)):
            self.document(row, source_id=125 + number,
                          text='Love others through compassion and practical care for neighbors.')
        context = knowledge.library_context_for_gemini('Suggest 5 books about loving others')
        titles = [source['title'] for source in context.approved_sources]
        self.assertEqual(titles.count('Love One Another'), 1)
        self.assertEqual(len(titles), 2)

    def test_truncated_import_titles_do_not_take_two_recommendation_slots(self):
        short_title = 'A Leaders Handbook for Local Church Volunt'
        full_title = 'A Leaders Handbook for Local Church Volunteers'
        for number, title in enumerate((short_title, full_title)):
            row = self.catalog(f'drive-{number}', 842 + number, 42 + number, title)
            self.document(row, source_id=125 + number,
                          text='Leadership means caring for people and supporting church volunteers.')
        with closing(resources.get_resource_db()) as db, db:
            db.execute("UPDATE pastor_library_books SET author='John Maxwell' WHERE id=42")
            db.execute("UPDATE pastor_library_books SET author='John C Maxwell PDF Converted' WHERE id=43")
        context = knowledge.library_context_for_gemini('Recommend books about leadership')
        self.assertEqual([source['title'] for source in context.approved_sources], [full_title])
        self.assertIn('/read/43?', context.approved_sources[0]['url'])
        first = dict(title=full_title, author='John Maxwell')
        self.assertFalse(knowledge._same_recommendation_title(first, dict(first, title=full_title + ' Volume 2')))
        self.assertFalse(knowledge._same_recommendation_title(first, dict(title=short_title, author='Another Writer')))

    def test_metadata_terms_match_words_instead_of_unrelated_substrings(self):
        self.assertFalse(knowledge._term_in_text('anger', 'danger zone'))
        self.assertFalse(knowledge._term_in_text('other', 'another book'))
        row = {'title': 'Danger Zone', 'author': 'Author', 'category': 'General', 'folder_path': ''}
        self.assertEqual(knowledge._metadata_score(row, ['anger'], 'recommendation'), 0)

    def test_study_bible_catalogue_supplies_verified_opening_material(self):
        row = self.catalog(title='Amplified Study Bible')
        self.document(row, text='Introduction. This Bible is designed for readers comparing word meanings. Its features include study notes.')
        context = knowledge.library_context_for_gemini('Suggest study Bibles')
        self.assertIn('OPENING EXCERPT:', context)
        self.assertIn('readers comparing word meanings', context)
        self.assertIn('Make the explanations distinct', context)
        self.assertLessEqual(len(context), knowledge.MAX_CONTEXT_CHARS)

    def test_common_topic_search_finishes_within_a_bounded_query_budget(self):
        row = self.catalog(title='Giving Guide')
        doc_id = self.document(row, text='Giving generously helps others.')
        with closing(knowledge._db()) as db:
            for table in ('pij_library_chunks', 'pij_library_chunks_fts'):
                db.execute(f'DELETE FROM {table} WHERE document_id=?', (doc_id,))
            db.executemany('''INSERT INTO pij_library_chunks
                (document_id,chunk_number,page_start,page_end,content) VALUES(?,?,?,?,?)''',
                ((doc_id, number, number, number, 'Giving generously helps others.') for number in range(1, 5001)))
            db.executemany('INSERT INTO pij_library_chunks_fts(document_id,chunk_number,content) VALUES(?,?,?)',
                ((doc_id, number, 'Giving generously helps others.') for number in range(1, 5001)))
            db.execute('UPDATE pij_library_documents SET chunk_count=5000 WHERE id=?', (doc_id,))
            db.commit()

            # A work budget catches whole-result sorting without depending on
            # machine speed. Scoped lookups should touch only the chosen book's
            # indexed chunk range, not traverse globally ranked FTS matches.
            for book_id, budget in ((None, 200000), (42, 20000)):
                with self.subTest(book_id=book_id):
                    steps = [0]

                    def interrupt_excessive_work():
                        steps[0] += 100
                        return int(steps[0] > budget)

                    db.set_progress_handler(interrupt_excessive_work, 100)
                    try:
                        hits = knowledge._search_chunks(db, ['giving'], limit=3, book_id=book_id)
                        self.assertEqual(len(hits), 3)
                        self.assertTrue(all(hit['book_id'] == 42 for hit in hits))
                    finally:
                        db.set_progress_handler(None, 0)

    def test_visibility_is_applied_before_the_ranked_result_limit(self):
        hidden = self.catalog(title='Hidden Giving Guide')
        self.document(hidden, text='Giving giving giving.')
        visible = self.catalog('drive-B', 843, 43, 'Available Giving Guide')
        self.document(visible, source_id=126, text='Giving generously helps support people in need.')
        with closing(knowledge._db()) as db:
            db.execute('UPDATE appdb.pastor_library_books SET is_hidden=1 WHERE id=42')
            db.commit()
            hits = knowledge._search_chunks(db, ['giving'], limit=1)
        self.assertEqual([hit['book_id'] for hit in hits], [43])

    def test_failed_and_empty_indexes_are_retried(self):
        row = self.catalog()
        self.document(row)
        db = knowledge._db()
        try:
            db.execute('UPDATE pij_library_documents SET searchable=0')
            db.commit()
            self.assertTrue(knowledge._needs_reindex(row))
            db.execute('UPDATE pij_library_documents SET searchable=1,chunk_count=0')
            db.commit()
            self.assertTrue(knowledge._needs_reindex(row))
        finally:
            db.close()
        with (patch.object(resources, 'get_drive_session', return_value=object()),
              patch.object(resources, 'download_drive_file_bytes', return_value=b'fake'),
              patch.object(knowledge, '_extract_pdf', return_value=[])):
            self.assertEqual(knowledge.index_public_library()['errors'], 1)

    def drive_item(self, drive_id='drive-A', name='Prayer Guide.pdf', **changes):
        item = dict(id=drive_id, name=name, mimeType='application/pdf', size='100',
                    createdTime='created', modifiedTime='2026-10-01T00:00:00Z',
                    sha256Checksum='a' * 64)
        item.update(changes)
        return item

    def sync_drive(self, items, recognizer=None):
        def list_folder(session, folder_id):
            return [dict(id='prayer-folder', name='Prayer', mimeType=resources.GOOGLE_DRIVE_FOLDER_MIME)] if folder_id == resources.PASTOR_RESOURCES_DRIVE_FOLDER_ID else copy.deepcopy(items)
        with (patch.object(resources, 'get_drive_session', return_value=object()),
             patch.object(resources, 'list_drive_folder', side_effect=list_folder),
             patch.object(resources, 'get_title_and_author', recognizer or Mock(return_value=('Prayer Guide', 'Author')))):
            return resources.sync_library_to_database_v3()

    def test_sync_skips_recognition_for_unchanged_files_and_preserves_hidden_state(self):
        self.catalog()
        db = resources.get_resource_db()
        db.execute("UPDATE pastor_library_books SET is_hidden=1,manual_title='Private title'")
        db.commit()
        db.close()
        result = self.sync_drive([self.drive_item()], Mock(side_effect=AssertionError('Recognition must be skipped')))
        self.assertEqual((result['unchanged_files'], result['new_files'], result['changed_files']), (1, 0, 0))
        self.assertEqual(result['unique_books'], 0)
        resources.invalidate_thumbnail_cache.assert_not_called()
        db = resources.get_resource_db()
        try:
            book = db.execute('SELECT * FROM pastor_library_books').fetchone()
            self.assertEqual((book['is_hidden'], book['manual_title'], book['is_active']), (1, 'Private title', 1))
        finally:
            db.close()

    def test_complete_drive_scan_deletes_missing_index_and_files_but_keeps_personal_saves(self):
        keep=self.catalog()
        missing=self.catalog('missing',843,43,'Missing guide')
        self.document(keep);missing_id=self.document(missing,source_id=126)
        db=knowledge._db()
        db.execute('INSERT INTO pij_library_source_formats VALUES (?,?,?,?,?)',(missing_id,'hash','now',1,'[]'))
        db.execute("INSERT INTO pij_library_documents(source_type,source_file_id,title,indexed_at) VALUES ('private_sermon',900,'Private sermon','now')")
        db.commit();db.close()
        resources.add_bookmark('reader',43,'PDF',locator='',page=2,label='Keep my bookmark',excerpt='My saved excerpt')
        result=self.sync_drive([self.drive_item()])
        self.assertEqual(result['removed_index_documents'],1)
        db=knowledge._db()
        try:
            self.assertIsNone(knowledge._find_public_document(db,'missing'))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pij_library_chunks').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pij_library_chunks_fts').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pij_library_source_formats').fetchone()[0],0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM pij_library_documents WHERE source_type='private_sermon'").fetchone()[0],1)
        finally:db.close()
        saved=resources.get_bookmarks('reader')
        self.assertEqual(saved[0]['excerpt'],'My saved excerpt')
        self.assertEqual(saved[0]['book_title'],'Missing guide')
        self.assertEqual(resources.search_library_database_v3('reader')['total'],1)

    def test_failed_or_cancelled_drive_scan_never_prunes_catalog_or_index(self):
        row=self.catalog();self.document(row)
        for failure in (RuntimeError('Drive unavailable'),resources.ResourceSyncCancelled()):
            with (patch.object(resources,'get_drive_session',return_value=object()),
                  patch.object(resources,'list_drive_folder',side_effect=failure),
                  patch.object(resources,'_prune_missing_library_sources') as prune):
                with self.assertRaises(type(failure)):resources.sync_library_to_database_v3()
                prune.assert_not_called()
            self.assertEqual(len(self.chunks()[0]),1)
            self.assertEqual(resources.search_library_database_v3('reader')['total'],1)

    def test_cancel_after_cleanup_rolls_back_both_databases(self):
        row=self.catalog();self.document(row)
        original=resources._prune_missing_library_sources
        def cancel(db,ids):
            original(db,ids)
            raise resources.ResourceSyncCancelled()
        with patch.object(resources,'_prune_missing_library_sources',side_effect=cancel):
            with self.assertRaises(resources.ResourceSyncCancelled):self.sync_drive([])
        self.assertEqual(len(self.chunks()[0]),1)
        self.assertEqual(resources.search_library_database_v3('reader')['total'],1)

    def test_original_epub_typography_keeps_safe_marks_and_paragraph_boundaries(self):
        output=io.BytesIO()
        with zipfile.ZipFile(output,'w') as archive:
            archive.writestr('META-INF/container.xml','<container><rootfile full-path="book.opf"/></container>')
            archive.writestr('book.opf','<package><manifest><item id="body" href="body.xhtml"/></manifest><spine><itemref idref="body"/></spine></package>')
            archive.writestr('style.css','.emphasis {font-weight:700;} .slant {font-style:italic;}')
            archive.writestr('body.xhtml','<html><head><style>.extra {font-weight:bold;}</style></head><body><h1>A theme</h1><p>Original <span class="emphasis">bold</span> and <i>italic</i>&nbsp;text.</p><script>unsafe()</script><p class="slant">Second paragraph.</p></body></html>')
        sections=knowledge._formatted_source_sections(output.getvalue(),'epub')
        blocks=sections[0]
        self.assertEqual([b['text'] for b in blocks],['A theme','Original bold and italic\u00a0text.','Second paragraph.'])
        self.assertIn('strong',next(r for r in blocks[1]['runs'] if r['text']=='bold')['marks'])
        self.assertIn('em',next(r for r in blocks[1]['runs'] if r['text']=='italic')['marks'])
        self.assertTrue(all('em' in run['marks'] for run in blocks[2]['runs']))
        for block in blocks:self.assertEqual(''.join(r['text'] for r in block['runs']),block['text'])

    def test_pdf_typography_reads_font_weight_and_italics(self):
        from reportlab.pdfgen.canvas import Canvas
        output=io.BytesIO();canvas=Canvas(output)
        canvas.setFont('Helvetica-Bold',14);canvas.drawString(20,800,'Original heading')
        canvas.setFont('Helvetica-Oblique',11);canvas.drawString(20,770,'Original italic paragraph')
        canvas.save()
        blocks=knowledge._formatted_source_sections(output.getvalue(),'pdf')[0]
        self.assertIn('strong',blocks[0]['runs'][0]['marks'])
        self.assertIn('em',blocks[1]['runs'][0]['marks'])

    def test_epub_outline_preserves_nested_lists_numbers_and_item_continuations(self):
        output=io.BytesIO()
        with zipfile.ZipFile(output,'w') as archive:
            archive.writestr('META-INF/container.xml','<container><rootfile full-path="book.opf"/></container>')
            archive.writestr('book.opf','<package><manifest><item id="body" href="body.xhtml"/></manifest><spine><itemref idref="body"/></spine></package>')
            archive.writestr('body.xhtml','<html><body><ol start="3" type="I"><li><p>Third point.</p><ul><li><em>Nested support.</em></li></ul><p>Continuation.</p></li><li value="7">Seventh point.</li></ol><p>After outline.</p></body></html>')
        blocks=knowledge._formatted_source_sections(output.getvalue(),'epub')[0]
        self.assertEqual([b['text'] for b in blocks],['Third point.','Nested support.','Continuation.','Seventh point.','After outline.'])
        self.assertEqual((blocks[0]['list_kind'],blocks[0]['list_number'],blocks[0]['list_style']),('ordered',3,'I'))
        self.assertEqual((blocks[1]['list_kind'],blocks[1]['list_level']),('bullet',2))
        self.assertEqual(blocks[0]['list_item'],blocks[2]['list_item'])
        self.assertNotEqual(blocks[0]['list_item'],blocks[3]['list_item'])
        self.assertEqual(blocks[3]['list_number'],7)
        self.assertNotIn('list_kind',blocks[4])
        self.assertIn('em',blocks[1]['runs'][0]['marks'])

    def test_invalid_pdf_surrogate_is_replaced_without_losing_remaining_source_text(self):
        page=SimpleNamespace(extract_text=lambda visitor_text:visitor_text('Original \ud800 text.\n',None,None,{},12))
        with patch.object(knowledge,'PdfReader',return_value=SimpleNamespace(pages=[page])):
            blocks=knowledge._formatted_source_sections(b'fixture','pdf')[0]
        self.assertEqual(blocks[0]['text'],'Original \ufffd text.')
        json.dumps(blocks,ensure_ascii=False).encode('utf-8')

    def test_spurgeon_original_outline_hierarchy_and_bullets_survive_lazy_loading(self):
        import bible_study
        row=self.catalog(title='Spurgeons Sermon Outlines',fmt='EPUB');docid=self.document(row)
        def block(text,marks=(),**properties):return dict(text=text,runs=[dict(text=text,marks=list(marks))],**properties)
        source=[block('1. Hastening Lot',['strong']),block('When the morning arose, then the angels hastened Lot. Genesis 19:15',['em']),
                block('An original introductory paragraph with sufficient source text to be recognized as a complete message.'),
                block('I. THE RIGHTEOUS NEED TO BE HASTENED.',['strong']),block('1. In what? In obedience.',['em']),
                block('In coming out from the world.',list_kind='bullet',list_level=1,list_item=1),
                block('In seeking the good of their families.',list_kind='bullet',list_level=1,list_item=2),
                block('II. THE SINNERS NEED TO BE HASTENED.',['strong']),block('An original closing paragraph.')]
        sections=[[block('Table of Contents'),block('1. Hastening Lot')],source]
        db=knowledge._db();db.execute('INSERT INTO pij_library_source_formats VALUES (?,?,?,?,?)',
            (docid,row['checksum'],row['modified_time'],2,json.dumps(sections)));db.commit();db.close()
        catalogue=bible_study.sermon_catalogue(self.app.AI_DATABASE)
        self.assertEqual([s['title'] for s in catalogue],['Hastening Lot'])
        self.assertIn('Charles Spurgeon',catalogue[0]['author']);self.assertIn('Barry L. Davis',catalogue[0]['author'])
        rich=bible_study._rich_source_blocks(self.app.AI_DATABASE,catalogue[0])
        self.assertEqual([b['text'] for b in rich],[b['text'] for b in source[1:]])
        self.assertEqual(rich[0]['role'],'sermon_reference')
        self.assertEqual(sum(b['role']=='sermon_heading' for b in rich),2)
        self.assertEqual(sum(b.get('list_kind')=='bullet' for b in rich),2)
        self.assertIn('em',next(b for b in rich if b['role']=='sermon_subpoint')['runs'][0]['marks'])
        self.assertEqual(bible_study.scripture_references('II Samuel 7:27')[0]['book_id'],10)
        self.assertEqual(bible_study.scripture_references('I Chronicles 13:8, 12, 15:25')[-1]['chapter'],15)

    def test_52_sermons_restores_paragraphs_emphasis_and_printed_outline(self):
        import bible_study
        for title in ('52 Sermons From the Gospel of John','52 Sermons From the Book of Acts',
                      '52 Sermons About Jesus','52 Funeral Sermons-1'):
            self.assertTrue(knowledge._formatted_source_title(title))
        row=self.catalog(title='52 Sermons From the Gospel of John',fmt='EPUB');docid=self.document(row)
        def block(text,marks=()):return dict(text=text,runs=[dict(text=text,marks=list(marks))])
        source=[block('17. CHRIST AND THE HUNGRY MULTITUDE',['strong']),block('John 6:1-14',['strong']),
                block('ILLUSTRATION:',['strong']),block('A first original paragraph, long enough to be a complete introduction to the sermon.'),
                block('A separate original paragraph.'),block('1. THE FIRST PRINTED POINT',['strong']),
                block('A Scripture quotation. John 6:5',['em']),block('2. THE SECOND PRINTED POINT',['strong']),
                block('The original closing paragraph.')]
        db=knowledge._db();db.execute('INSERT INTO pij_library_source_formats VALUES (?,?,?,?,?)',
            (docid,row['checksum'],row['modified_time'],1,json.dumps([[block('Table of Contents'),block('17. CHRIST AND THE HUNGRY MULTITUDE')],source])));db.commit();db.close()
        catalogue=bible_study.sermon_catalogue(self.app.AI_DATABASE)
        self.assertEqual(len(catalogue),1)
        rich=bible_study._rich_source_blocks(self.app.AI_DATABASE,catalogue[0])
        self.assertEqual([b['text'] for b in rich],[b['text'] for b in source[1:]])
        self.assertEqual(rich[0]['role'],'sermon_reference')
        self.assertEqual(sum(b['role']=='sermon_heading' for b in rich),2)
        self.assertIn('em',rich[5]['runs'][0]['marks'])
        self.assertTrue(bible_study.sermons_for_chapter(self.app.AI_DATABASE,43,6))
        self.assertFalse(bible_study.sermons_for_chapter(self.app.AI_DATABASE,43,5))

    def test_52_sermons_keeps_whole_chapters_parts_and_excludes_closing_contents(self):
        import bible_study
        doc=dict(id=1,title='52 Sermons About Jesus',author='Printed author')
        rows=[(1,'Table of Contents\n1. OUR HOPE – PART 1\n2. OUR HOPE – PART 2'),
              (2,'1. OUR HOPE – PART 1\nJohn 11\nILLUSTRATION:\nA complete original illustration, followed by the printed sermon explanation and its application.'),
              (3,'2. OUR HOPE – PART 2\nILLUSTRATION:\nAn incidental quotation mentions John 3:16 while introducing the theme.\n1. A PRINTED POINT\nAn original Scripture quotation. – Acts 2:1\nThe complete original explanation follows the preaching text.'),
              (4,'INVESTING IN THIS BOOK!\nA publisher advertisement.'),
              (5,'Table of Contents\n1. OUR HOPE – PART 1\n2. OUR HOPE – PART 2\nTHANK YOU FOR INVESTING IN THIS BOOK!')]
        sections=bible_study._sermon_sections(doc,rows)
        self.assertEqual([s['title'] for s in sections],['OUR HOPE – PART 1','OUR HOPE – PART 2'])
        self.assertEqual(sections[0]['ranges'][0]['chapter'],11)
        self.assertEqual(sections[1]['ranges'][0]['book_id'],44)
        self.assertNotIn('advertisement',sections[-1]['text'])
        self.assertNotIn('Table of Contents',sections[-1]['text'])
        whole=sections[0]['blocks'][0]
        self.assertEqual(whole['text'],'John 11');self.assertTrue(whole['references'][0]['chapter_only'])

    def test_derived_sermon_catalogue_survives_restart_and_invalidates_on_source_change(self):
        import bible_study
        row=self.catalog(title='Sermon Resources Vol 1')
        docid=self.document(row,text='1. ORIGINAL THEME\nJohn 3:16\nA complete original sermon introduction with enough source explanation and a practical closing application.')
        first=bible_study.sermon_catalogue(self.app.AI_DATABASE)
        self.assertEqual([s['title'] for s in first],['ORIGINAL THEME'])
        bible_study._sermon_catalogue.cache_clear()
        with patch.object(bible_study,'_indexed_source',side_effect=AssertionError('A restart reparsed an unchanged book')):
            self.assertEqual(bible_study.sermon_catalogue(self.app.AI_DATABASE),first)
        cache=next((Path(self.app.AI_DATABASE).parent/'bible_study_data/catalogue_cache').glob('sermons-*.json.gz'))
        cache.write_bytes(b'broken derived cache')
        bible_study._sermon_catalogue.cache_clear()
        with patch.object(bible_study,'_indexed_source',wraps=bible_study._indexed_source) as reader:
            self.assertEqual(bible_study.sermon_catalogue(self.app.AI_DATABASE),first)
            self.assertEqual(reader.call_count,1)
        db=knowledge._db();db.execute('UPDATE pij_library_documents SET modified_time=? WHERE id=?',('changed',docid));db.commit();db.close()
        with patch.object(bible_study,'_indexed_source',wraps=bible_study._indexed_source) as reader:
            self.assertEqual(bible_study.sermon_catalogue(self.app.AI_DATABASE),first)
            self.assertEqual(reader.call_count,1)

    def test_rich_source_cache_retains_revision_emphasis_without_sharing_mutable_blocks(self):
        import bible_study
        row=self.catalog(title='52 Sermons About Jesus',fmt='EPUB');docid=self.document(row)
        text='A complete original paragraph with its printed wording unchanged.'
        def source(marks):return [[dict(text=text,runs=[dict(text=text,marks=marks)])]]
        db=knowledge._db();db.execute('INSERT INTO pij_library_source_formats VALUES(?,?,?,?,?)',
            (docid,row['checksum'],row['modified_time'],1,json.dumps(source(['em']))));db.commit();db.close()
        section=dict(document_id=docid,source_section=1,text=text,title='A theme',family='collection')
        first=bible_study._rich_source_blocks(self.app.AI_DATABASE,section)
        first[0]['runs'][0]['marks'].append('changed-by-caller')
        hits=bible_study._cached_rich_source_blocks.cache_info().hits
        second=bible_study._rich_source_blocks(self.app.AI_DATABASE,section)
        self.assertEqual(second[0]['runs'][0]['marks'],['em'])
        self.assertEqual(bible_study._cached_rich_source_blocks.cache_info().hits,hits+1)
        db=knowledge._db();db.execute('UPDATE pij_library_source_formats SET version=2,sections_json=? WHERE document_id=?',
            (json.dumps(source(['strong'])),docid));db.commit();db.close()
        self.assertEqual(bible_study._rich_source_blocks(self.app.AI_DATABASE,section)[0]['runs'][0]['marks'],['strong'])
        db=knowledge._db();db.execute('UPDATE pij_library_documents SET checksum=? WHERE id=?',('new-source',docid));db.commit();db.close()
        self.assertNotIn('runs',bible_study._rich_source_blocks(self.app.AI_DATABASE,section)[0],
                         'Outdated source typography survived a source revision')

    def test_indexed_page_cache_reads_only_opened_pages_and_recovers_from_corruption(self):
        import bible_study as b
        row=self.catalog(title='52 Sermons About Jesus',fmt='EPUB');docid=self.document(row)
        text='An original paragraph with its source emphasis preserved.'
        source=[[dict(text='Unopened page',runs=[])],[dict(text=text,runs=[dict(text=text,marks=['em'])])]]
        db=knowledge._db();db.execute('INSERT INTO pij_library_source_formats VALUES(?,?,?,?,?)',
            (docid,row['checksum'],row['modified_time'],1,json.dumps(source)));db.commit();db.close()
        fingerprint=(row['checksum'],row['modified_time'],row['checksum'],row['modified_time'],1)
        b._store_source_pages(self.app.AI_DATABASE,docid,fingerprint,source)
        section=dict(document_id=docid,source_section=2,text=text,title='Theme',family='collection')
        connect=sqlite3.connect
        def forbid_whole_volume(*args,**kwargs):
            conn=connect(*args,**kwargs)
            conn.set_authorizer(lambda action,table,column,*_:sqlite3.SQLITE_DENY
                if action==sqlite3.SQLITE_READ and table=='pij_library_source_formats' and column=='sections_json' else sqlite3.SQLITE_OK)
            return conn
        with patch.object(b.sqlite3,'connect',side_effect=forbid_whole_volume):
            first=b._rich_source_blocks(self.app.AI_DATABASE,section)
        self.assertEqual(first[0]['runs'][0]['marks'],['em'])
        b._source_page_path(self.app.AI_DATABASE,docid,fingerprint).write_bytes(b'broken derived cache')
        b._cached_rich_source_blocks.cache_clear()
        self.assertEqual(b._rich_source_blocks(self.app.AI_DATABASE,section),first)
        self.assertIsNone(b._read_source_pages(self.app.AI_DATABASE,docid,('changed',*fingerprint[1:]),2,2))

    def test_wiersbe_native_paragraphs_and_heading_classes_survive_continuation_pages(self):
        import bible_study as b
        row=self.catalog(title='Be Complete (Colossians)',fmt='EPUB');docid=self.document(row)
        def block(text,marks=(),**properties):return dict(text=text,runs=[dict(text=text,marks=list(marks))],**properties)
        paragraphs=[block('If you received an original letter, this paragraph retains its printed wording and punctuation.'),
                    block('1. SHARING THE GOSPEL',source_heading=2),
                    dict(text='Their past alienation. This is still one complete source paragraph.',runs=[dict(text='Their past alienation.',marks=['strong']),dict(text=' This is still one complete source paragraph.',marks=[])])]
        continuation=block('An original continuation paragraph on the next source page.')
        source=[[block('Chapter Five'),block('One Man’s Ministry',source_heading=1),block('(Colossians 1:21—2:3)'),*paragraphs],[continuation]]
        db=knowledge._db();db.execute("UPDATE pij_library_documents SET author='Warren W. Wiersbe' WHERE id=?",(docid,))
        db.execute('INSERT INTO pij_library_source_formats VALUES(?,?,?,?,?)',(docid,row['checksum'],row['modified_time'],1,json.dumps(source)));db.commit();db.close()
        entry=b.wiersbe_commentary(self.app.AI_DATABASE,51,2,1)['entries'][0]
        self.assertEqual([x['text'] for x in entry['blocks']],[x['text'] for x in paragraphs]+[continuation['text']])
        self.assertEqual(entry['blocks'][1]['role'],'commentary_heading')
        self.assertEqual(entry['blocks'][2]['type'],'paragraph')
        self.assertEqual(entry['blocks'][2]['runs'][0]['marks'],['strong'])
        self.assertTrue(knowledge._formatted_source_title(row['title'],'Warren W. Wiersbe','EPUB'))
        self.assertFalse(knowledge._formatted_source_title(row['title'],'Warren W. Wiersbe','PDF'))

    def test_staff_keeps_teaching_chapters_and_uses_main_passages_without_incidental_coverage(self):
        import bible_study as b
        doc=dict(id=1,title='The-Shepherds-Staff')
        rows=[(1,'Chapter 1 - Prayer\nChapter 1\nPrayer\nAn opening quotation. Psalms 91:15-16\nA. THE SECRET PLACE\nAn original teaching quotation. Matthew 6:6\n1. Right motives\nAn incidental illustration mentions John 3:16, but is not a main teaching text.'),
              (2,'The original continuation of the same complete teaching chapter.\nB. LISTENING\nA main teaching quotation. Luke 11:1\n1. A printed subpoint\nThe remaining original explanation.'),
              (3,'TOPICAL CONCORDANCE\nJohn 3:16\nA different reference section must not become part of the Prayer chapter.')]
        sections=b._sermon_sections(doc,rows)
        self.assertEqual([x['title'] for x in sections],['Prayer'])
        self.assertEqual([(x['book_id'],x['chapter']) for x in sections[0]['ranges']],[(19,91),(40,6),(42,11)])
        self.assertEqual(sections[0]['source_end_section'],2)
        self.assertIn('same complete teaching chapter',sections[0]['text'])
        self.assertNotIn('TOPICAL CONCORDANCE',sections[0]['text'])
        self.assertEqual(sum(x.get('role')=='sermon_heading' for x in sections[0]['blocks']),2)

    def test_staff_uses_printed_contributor_and_numbered_point_scripture(self):
        import bible_study as b
        doc=dict(id=1,title='The-Shepherds-Staff')
        rows=[(1,'SECTION Al\nA SOURCE SECTION\nBy Printed Contributor\nChapter 1\nMethods Of\nEvangelism\nIntroduction\nAn original teaching introduction without a passage.\nA. TWO METHODS\n1. Mass Evangelism\n"An opening Scripture quotation describes preaching." Acts 8:5-8\nThe original explanation of the teaching continues here.\nSECTION A2\nANOTHER SECTION\nBy Another Contributor')]
        sections=b._sermon_sections(doc,rows,{'Evangelism':True})
        self.assertEqual(len(sections),1)
        self.assertEqual(sections[0]['title'],'Methods Of Evangelism')
        self.assertIn('Printed Contributor',sections[0]['author'])
        self.assertNotIn('Another Contributor',sections[0]['text'])
        self.assertEqual(sections[0]['ranges'][0]['book_id'],44)
        self.assertFalse(any(x['text'] in ('Methods Of','Evangelism') for x in sections[0]['blocks']))

    def test_bible_sources_follow_live_catalog_even_before_a_cleanup(self):
        import bible_study
        row=self.catalog(title='Sermon Resources Vol 1')
        self.document(row,text='1. A PRINTED THEME\nJohn 3:16\nA complete original sermon introduction with sufficient explanation.\nI. BELIEVE\nThe remaining original preaching outline and its explanation.')
        self.assertTrue(bible_study.sermons_for_chapter(self.app.AI_DATABASE,43,3))
        db=resources.get_resource_db();db.execute('UPDATE pastor_library_books SET is_hidden=1');db.commit();db.close()
        self.assertFalse(bible_study.sermons_for_chapter(self.app.AI_DATABASE,43,3))
        db=resources.get_resource_db();db.execute('UPDATE pastor_library_books SET is_hidden=0');db.execute('UPDATE pastor_library_files SET is_active=0');db.commit();db.close()
        self.assertFalse(bible_study.sermons_for_chapter(self.app.AI_DATABASE,43,3))
        self.assertEqual(len(self.chunks()[0]),1,'Ordinary reading must not delete retained index text')

    def test_pulpit_catalogue_stays_small_and_reads_original_emphasis_for_open_article(self):
        import bible_study
        row=self.catalog(title='The Pulpit Commentary Complete Volume 9')
        docid=self.document(row)
        def block(text,bold=False):return dict(text=text,runs=[dict(text=text,marks=['strong'] if bold else [])])
        blocks=[block('HOMILETICS',True),block('Colossians 2:6-7'),block('Walking in Christ.',True),
                block('The original homily begins here and explains how Christians continue to walk in Christ.'),
                block('A supporting quotation ends with the following reference:'),block('John 3:16).'),
                block('(2) An outline subpoint, not another sermon title.',True),
                block('All the rest of this original homily should remain inside this message and retain its emphasis.')]
        db=knowledge._db()
        db.execute('INSERT INTO pij_library_source_formats VALUES (?,?,?,?,?)',
            (docid,row['checksum'],row['modified_time'],1,json.dumps([blocks])))
        db.commit();db.close()
        catalogue=bible_study.sermon_catalogue(self.app.AI_DATABASE)
        self.assertEqual([s['title'] for s in catalogue],['Walking in Christ.'])
        self.assertFalse(catalogue[0].get('blocks'),'The whole catalogue must not retain rich article blocks')
        rich=bible_study._rich_source_blocks(self.app.AI_DATABASE,catalogue[0])
        self.assertIn('strong',next(b for b in rich if b['text']=='Walking in Christ.')['runs'][0]['marks'])
        self.assertIn('All the rest',' '.join(b['text'] for b in rich))
        self.assertFalse(bible_study.sermons_for_chapter(self.app.AI_DATABASE,43,3),'A supporting citation became a sermon suggestion')

    def test_sync_detects_new_modified_removed_renamed_and_moved_files(self):
        self.catalog()
        self.catalog('removed', 843, 43, 'Removed Guide')
        recognize = Mock(side_effect=lambda name, folder: (name.rsplit('.', 1)[0], 'Author'))
        items = [self.drive_item(name='Renamed Guide.pdf', modifiedTime='updated', sha256Checksum='b' * 64),
                 self.drive_item('new-drive', 'New Guide.pdf', sha256Checksum='c' * 64)]
        result = self.sync_drive(items, recognize)
        self.assertEqual((result['changed_files'], result['new_files'], result['removed_files']), (1, 1, 1))
        self.assertEqual(recognize.call_count, 2)
        self.assertEqual(result['unique_books'], 2)
        db = resources.get_resource_db()
        try:
            self.assertIsNone(db.execute("SELECT is_active FROM pastor_library_files WHERE drive_file_id='removed'").fetchone())
        finally:
            db.close()
        # A folder move without a modified-time change also needs recognition.
        recognize.reset_mock()
        with (patch.object(resources, 'get_drive_session', return_value=object()),
             patch.object(resources, 'list_drive_folder', return_value=items),
             patch.object(resources, 'get_title_and_author', recognize)):
            result = resources.sync_library_to_database_v3()
        self.assertEqual(result['changed_files'], 2)
        self.assertEqual(recognize.call_count, 2)

    def test_sync_repeated_duplicate_and_pdf_epub_pairing_is_stable(self):
        items = [self.drive_item(), self.drive_item('duplicate', 'Duplicate.pdf'),
                 self.drive_item('epub', 'Prayer Guide.epub', mimeType='application/epub+zip', sha256Checksum='b' * 64)]
        first = self.sync_drive(items)
        second = self.sync_drive(items)
        self.assertEqual((first['unique_books'], second['unique_books']), (1, 1))
        self.assertEqual((second['exact_duplicates'], second['unchanged_files']), (1, 3))
        db = resources.get_resource_db()
        try:
            rows = db.execute('SELECT * FROM pastor_library_files').fetchall()
            self.assertEqual(len(rows), 3)
            self.assertEqual(len({row['book_id'] for row in rows}), 1)
            self.assertEqual(sum(row['is_duplicate'] for row in rows), 1)
        finally:
            db.close()

    def test_catalog_failure_rolls_back_and_does_not_deactivate_missing_files(self):
        self.catalog()
        with (patch.object(resources, 'get_drive_session', return_value=object()),
             patch.object(resources, 'list_drive_folder', side_effect=RuntimeError('Drive unavailable'))):
            with self.assertRaisesRegex(RuntimeError, 'Drive unavailable'):
                resources.sync_library_to_database_v3()
        db = resources.get_resource_db()
        try:
            self.assertEqual(db.execute('SELECT is_active FROM pastor_library_files').fetchone()[0], 1)
        finally:
            db.close()

    def test_progress_persistence_is_throttled_and_new_counters_survive_workers(self):
        with (patch.object(resources, 'RESOURCE_SYNC_STATE', dict(resources.RESOURCE_SYNC_STATE)),
             patch.object(resources, 'RESOURCE_SYNC_LAST_PERSISTED', 0.0),
             patch.object(resources.time, 'monotonic', return_value=100.0),
             patch.object(resources, '_persist_resource_sync_state') as persist):
            for checked in range(100):
                resources.update_resource_sync_state(running=True, stage='syncing', processed=checked)
            self.assertEqual(persist.call_count, 1)
            resources.update_resource_sync_state(running=False, stage='complete', removed_files=2, errors=1)
            self.assertEqual(persist.call_count, 2)
        resources._persist_resource_sync_state(dict(resources.RESOURCE_SYNC_STATE,
                                                   stats={'removed_files': 2, 'errors': 1}))
        state = resources._read_persisted_resource_sync_state()
        self.assertEqual((state['removed_files'], state['errors']), (2, 1))


class BibleStudyIntegrationTests(unittest.TestCase):
    """Phase 2A tests never import app.py or access website databases."""

    def setUp(self):
        import bible_study
        from jinja2 import ChoiceLoader, DictLoader, FileSystemLoader
        self.bible = bible_study
        self.directory = tempfile.TemporaryDirectory(prefix='district4_bible_test_')
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'bible_study.db'
        self.app = Flask(__name__, static_folder=str(Path(resources.__file__).parent / 'static'))
        self.app.secret_key = 'isolated-test-session-key-not-production'
        self.app.config.update(TESTING=True, BIBLE_STUDY_DATABASE=str(self.path), BIBLE_STUDY_TRANSLATION_FOLDER_ID='',
                               AI_DATABASE=str(Path(self.directory.name)/'ai.db'))
        self.app.jinja_loader = ChoiceLoader([
            DictLoader({'base.html': '<html><head></head><body>{% block content %}{% endblock %}</body></html>'}),
            FileSystemLoader(str(Path(resources.__file__).parent / 'templates')),
        ])
        self.app.add_url_rule('/', endpoint='splash', view_func=lambda: 'Login')
        self.app.add_url_rule('/pastor-resources', endpoint='pastor_resources', view_func=lambda: 'Library')
        self.bible.register_bible_study_routes(self.app)
        self.client = self.app.test_client()
        self.base = '/pastor-resources/bible-study'

    def login(self, username='Pijeme', role='Area Overseer', pastor=False):
        with self.client.session_transaction() as account:
            account.clear()
            if pastor:
                account.update(pastor_logged_in=True, pastor_username=username)
            else:
                account.update(ao_logged_in=True, ao_username=username, ao_role=role)

    def preview(self, initialize=False):
        self.login()
        if initialize:
            self.app.bible_study_init_schema()
        self.assertEqual(self.client.get(self.base).status_code, 200)
        with self.client.session_transaction() as account:
            return {'X-CSRF-Token': account['bible_study_csrf']}

    def settings(self, **updates):
        return {**dict(book='Genesis', chapter=1, verse=1, theme='paper', font_size=19,
                       layout_mode='split', split_ratio=60, version=0), **updates}

    def translation_zip(self, word='Ginoo', malformed=False):
        # Synthetic USFM; no licensed Bible text enters tests or AI fixtures.
        output=io.BytesIO()
        with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as archive:
            for code, (_,chapters) in zip(self.bible.USFM_BOOK_IDS,self.bible.BOOKS):
                text='\\id '+code+'\n'
                for chapter in range(1,chapters+1):
                    text+=f'\\c {chapter}\n\\s1 Synthetic heading\n\\p\n\\v 1 {word} synthetic.\n'
                    if code=='GEN' and chapter==1:
                        text+='\\s2 Second heading\n\\q1\n\\v 2-3 '+word+' synthetic\n\\q2 next line. \\f + \\fr 1:2 \\ft Synthetic note.\\f*\n'
                        text+='\\v 4 Ginoong pag-ibig synthetic.\n'
                archive.writestr(code+'.usfm', text if not malformed or code!='GEN' else text.replace('\\v 2-3','\\v 1-3',1))
        return output.getvalue()

    def test_usfm_validation_preserves_ranges_headings_footnotes_and_poetry(self):
        data=self.translation_zip()
        parsed=self.bible.parse_usfm_archive(data,'CEBOCB')
        self.assertEqual((parsed['books'],parsed['chapters'],parsed['verse_ranges']), (66,1189,1))
        first,combined,last=parsed['verses'][:3]
        self.assertEqual((combined['verse'],combined['verse_end']), (2,3))
        self.assertEqual(first['heading'],'Synthetic heading')
        self.assertEqual(combined['heading'],'Second heading')
        self.assertEqual(last['heading'],'')
        self.assertEqual(combined['text'],'Ginoo synthetic\nnext line.')
        self.assertEqual(combined['notes'],[{'text':'1:2 Synthetic note.'}])
        self.assertEqual(combined['layout'],'poetry')
        with self.assertRaises(self.bible.SyncError):
            self.bible.parse_usfm_archive(self.translation_zip(malformed=True),'CEBOCB')
        with self.assertRaises(self.bible.SyncError):
            self.bible.parse_usfm_archive(b'broken','TCB')
        with self.assertRaises(self.bible.SyncError):
            self.bible.parse_usfm_archive(data,'NIV')

    def test_translation_import_reader_concordance_preferences_and_edition_isolation(self):
        headers=self.preview(initialize=True)
        self.publish_fixture()
        with closing(sqlite3.connect(self.path)) as conn:
            old=conn.execute('SELECT id,text FROM bible_verses ORDER BY id').fetchall()
        data=self.translation_zip()
        with patch.object(knowledge,'index_public_library',side_effect=AssertionError('No AI index access')):
            result=self.bible.import_usfm_translations(self.path,{'CEBOCB':data,'TCB':self.translation_zip('Panginoon')})
        self.assertTrue(Path(result['backup']).is_file())
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT id,text FROM bible_verses WHERE id<=? ORDER BY id',(old[-1][0],)).fetchall(),old)
            self.assertIsNone(conn.execute('PRAGMA foreign_key_check').fetchone())
        query='?book=Genesis&chapter=1&translation=CEBOCB'
        chapter=self.client.get(self.base+'/api/chapter'+query).json
        self.assertEqual(len(chapter['verses']),3)
        verse=chapter['verses'][1]
        self.assertEqual((verse['verse'],verse['verse_end']),(2,3))
        self.assertEqual(verse['footnotes'][0]['text'],'1:2 Synthetic note.')
        preview=self.client.get(self.base+'/api/verse'+query+'&verse=3').json['verse']
        self.assertEqual(preview['reference'],'Genesis 1:2–3')
        self.assertEqual(preview['text'],verse['text'])
        self.assertIsNone(self.bible.get_indexed_niv_passage('Genesis 1:2',self.path))
        matches=self.client.get(self.base+'/api/concordance?q=GINOO&translation=CEBOCB').json
        self.assertEqual(len(matches['entries']),20)
        self.assertEqual(matches['next_offset'],20)
        self.assertEqual(matches['entries'][1]['headword'],'Genesis 1:2–3')
        self.assertNotIn('Ginoong',str(matches['entries']))
        exact=self.client.get(self.base+'/api/concordance?q=pag-ibig&translation=CEBOCB').json
        self.assertEqual([v['headword'] for v in exact['entries']],['Genesis 1:4'])
        other=self.client.get(self.base+'/api/concordance?q=Ginoo&translation=TCB').json
        self.assertEqual(other['entries'],[])
        native=self.client.get(self.base+'/api/word/'+str(verse['id'])+'/0').json
        self.assertEqual(native['result_type'],'native_dictionary');self.assertFalse(native['verified'])
        self.assertNotIn('original_tokens',native)
        self.assertEqual(self.client.get(self.base+'/api/chapter?book=Genesis&chapter=1&translation=bad').status_code,400)
        saved=self.client.post(self.base+'/api/state',json=self.settings(translation='CEBOCB',verse=3),headers=headers)
        self.assertEqual(saved.status_code,200)
        self.assertEqual(self.client.get(self.base+'/api/state').json['state']['translation'],'CEBOCB')
        ranges=[dict(verse_id=verse['id'],start_offset=0,end_offset=5,selected_text='Ginoo')]
        added=self.client.post(self.base+'/api/annotations',json=dict(kind='highlight',color='#ffe66d',ranges=ranges),headers=headers)
        self.assertEqual(added.status_code,201,added.json)
        self.assertEqual(len(self.client.get(self.base+'/api/personal'+query).json['annotations']),1)
        self.assertEqual(self.client.get(self.base+'/api/personal?book=Genesis&chapter=1&translation=TCB').json['annotations'],[])
        again=self.bible.import_usfm_translations(self.path,{'CEBOCB':data})
        self.assertIsNone(again['backup'])
        self.login('Another Reader',pastor=True)
        self.assertEqual(self.client.get(self.base+'/api/state').json['state']['translation'],'NIV')
        self.assertEqual(self.client.get(self.base+'/api/personal'+query).json['annotations'],[])

    def test_translation_validation_failure_preserves_existing_database(self):
        self.preview(initialize=True);self.publish_fixture()
        with closing(sqlite3.connect(self.path)) as conn:
            before=conn.execute('SELECT * FROM bible_verses').fetchall()
        with self.assertRaises(self.bible.SyncError):
            self.bible.import_usfm_translations(self.path,{'CEBOCB':self.translation_zip(),'TCB':b'bad'})
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT * FROM bible_verses').fetchall(),before)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_editions').fetchone()[0],2)

    def test_v6_translation_upgrade_preserves_verses_tokens_notes_bookmarks_and_state(self):
        legacy=self.bible.SCHEMA_SQL.replace("'NIV','KJV','CEBOCB','TCB'", "'NIV','KJV'")
        legacy=legacy.replace("    verse_end INTEGER NOT NULL DEFAULT 0, notes_json TEXT NOT NULL DEFAULT '[]', layout TEXT NOT NULL DEFAULT '',\n",'')
        legacy=legacy.replace("    normalized_text TEXT NOT NULL DEFAULT '',\n",'').replace('CREATE INDEX bible_token_word ON bible_tokens(normalized_text,verse_id);','')
        with patch.object(self.bible,'SCHEMA_SQL',legacy):
            headers=self.preview(initialize=True)
        self.publish_fixture()
        with closing(sqlite3.connect(self.path)) as conn,conn:
            conn.execute('PRAGMA user_version=6')
        verse=self.client.get(self.base+'/api/chapter').json['verses'][0]
        self.client.post(self.base+'/api/bookmarks',json={'verse_id':verse['id']},headers=headers)
        self.client.post(self.base+'/api/annotations',json=dict(kind='note',note='Keep this note',ranges=[dict(verse_id=verse['id'],start_offset=0,end_offset=5,selected_text='Alpha')]),headers=headers)
        self.client.post(self.base+'/api/state',json=self.settings(),headers=headers)
        tables=('bible_verses','bible_tokens','bible_annotations','bible_bookmarks','bible_reader_state')
        with closing(sqlite3.connect(self.path)) as conn,conn:
            conn.execute('PRAGMA user_version=6')
            columns={table:[row[1] for row in conn.execute('PRAGMA table_info('+table+')')] for table in tables}
            before={table:conn.execute('SELECT '+','.join(columns[table])+' FROM '+table+' ORDER BY rowid').fetchall() for table in tables}
        result=self.bible.import_usfm_translations(self.path,{'CEBOCB':self.translation_zip()})
        with closing(sqlite3.connect(result['backup'])) as backup:
            self.assertEqual(backup.execute('PRAGMA user_version').fetchone()[0],6)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0],self.bible.SCHEMA_VERSION)
            self.assertIsNone(conn.execute('PRAGMA foreign_key_check').fetchone())
            for table in tables:
                rows=conn.execute('SELECT '+','.join(columns[table])+' FROM '+table+' ORDER BY rowid').fetchall()
                self.assertEqual(rows[:len(before[table])],before[table])
        self.assertEqual(self.client.get(self.base+'/api/state').json['state']['translation'],'NIV')

    def test_combined_translation_verse_inherits_chain_topics_from_each_number(self):
        self.preview(initialize=True);self.publish_fixture()
        self.bible.import_usfm_translations(self.path,{'CEBOCB':self.translation_zip()})
        with closing(sqlite3.connect(self.path)) as conn,conn:
            edition=conn.execute("SELECT id FROM bible_editions WHERE translation='NIV'").fetchone()[0]
            for number in (2,3):
                verse=conn.execute('''INSERT INTO bible_verses(edition_id,book_id,chapter,verse,text,content_hash)
                    VALUES(?,1,1,?,'Synthetic English text',?)''',(edition,number,str(number)*64)).lastrowid
                topic=conn.execute('''INSERT INTO bible_chain_topics(edition_id,topic_key,title,content_json,source_page)
                    VALUES(?,?,?,'{}',1)''',(edition,str(number),'Synthetic topic '+str(number))).lastrowid
                conn.execute('INSERT INTO bible_chain_verses(verse_id,topic_id) VALUES(?,?)',(verse,topic))
        chapter=self.client.get(self.base+'/api/chapter?translation=CEBOCB').json
        self.assertEqual([item['topic_key'] for item in chapter['verses'][1]['topics']],['2','3'])
        self.assertEqual(chapter['verses'][0]['topics'],[])

    def test_translation_zip_drive_sync_is_separate_and_repeatable(self):
        self.preview(initialize=True)
        fixture=self.translation_zip()
        items=[dict(id='synthetic-cebuano-zip',name='cebocb_usfm.zip'),dict(id='synthetic-tagalog-zip',name='Open Tagalog Bible.zip')]
        drive=Mock()
        with patch.object(resources,'list_drive_folder',return_value=items) as listing:
            result=self.bible.sync_translation_sources(self.path,'restricted-folder',lambda message:None,drive=drive,fetch=lambda source:fixture)
        listing.assert_called_once_with(drive,'restricted-folder')
        self.assertEqual(set(result['counts']),{'CEBOCB','TCB'})
        # An edition hash is independent of the other translation's identical test archive.
        self.assertEqual(len(self.client.get(self.base+'/api/translations').json['translations']),3)
        with patch.object(resources,'list_drive_folder',return_value=items+[items[0]]):
            with self.assertRaises(self.bible.SyncError):
                self.bible.sync_translation_sources(self.path,'restricted-folder',lambda message:None,drive=drive,fetch=lambda source:fixture)

    def test_anonymous_requests_never_open_or_create_storage(self):
        with patch.object(self.bible.sqlite3, 'connect', side_effect=AssertionError('No database access')):
            self.assertEqual(self.client.get(self.base).status_code, 302)
            for route in ('/api/status', '/api/state'):
                self.assertEqual(self.client.get(self.base + route).status_code, 401)
        self.assertFalse(self.path.exists())

    def test_old_preview_flags_do_not_hide_the_module(self):
        self.login('Regular Pastor', pastor=True)
        self.app.config.update(BIBLE_STUDY_ENABLED='0', BIBLE_STUDY_PREVIEW_ENABLED='0')
        self.assertEqual(self.client.get(self.base).status_code, 200)
        self.assertEqual(self.client.get(self.base + '/api/status').status_code, 200)
        self.assertFalse(self.path.exists())

    def test_access_requires_authenticated_library_account_not_just_username(self):
        for account in (
            {'username': 'Pijeme', 'role': 'Area Overseer'},
            {'ao_logged_in': 'true', 'ao_username': 'Pijeme', 'ao_role': 'Area Overseer'},
            {'ao_logged_in': True},
            {'pastor_logged_in': True},
            {'member_logged_in': True, 'username': 'Member'},
        ):
            with self.client.session_transaction() as session:
                session.clear()
                session.update(account)
            self.assertEqual(self.client.get(self.base + '/api/status').status_code, 401)
        for username, pastor in (('Pijeme', False), ('OtherAO', False), ('Pastor', True)):
            with self.subTest(username=username):
                self.login(username, pastor=pastor)
                self.assertEqual(self.client.get(self.base).status_code, 200)
                self.assertEqual(self.client.get(self.base + '/api/status').status_code, 200)

    def test_missing_database_is_an_empty_preview_and_is_not_created(self):
        self.preview()
        with patch.object(self.bible.sqlite3, 'connect', side_effect=AssertionError('No implicit creation')):
            status = self.client.get(self.base + '/api/status')
            state = self.client.get(self.base + '/api/state')
        self.assertEqual(status.json['status'], 'not_initialized')
        self.assertEqual(status.json['indexed_verses'], 0)
        self.assertFalse(state.json['storage_available'])
        self.assertEqual(status.headers['Cache-Control'], 'private, no-store')
        self.assertNotIn(str(self.path), status.get_data(as_text=True))
        self.assertFalse(self.path.exists())

    def version_seven_fixture(self):
        # Model the deployed pre-resource database, without any v8-only tables.
        dictionary_schema = self.bible.DICTIONARY_SCHEMA_SQL.replace(
            "('mounce','vine','harper','tagalog','cebuano')", "('mounce','vine')")
        with (patch.object(self.bible, 'SCHEMA_VERSION', 7),
              patch.object(self.bible, 'STUDY_RESOURCE_SCHEMA_SQL', ''),
              patch.object(self.bible, 'DICTIONARY_SCHEMA_SQL', dictionary_schema)):
            self.bible.initialize_database(self.path)
        self.publish_fixture()
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("INSERT INTO bible_reader_state(user_key,updated_at,progress_percent) VALUES('pastor:fixture','now',45)")
        return self.preview()

    def test_version_seven_reader_and_running_sync_stay_visible_without_migration(self):
        headers = self.version_seven_fixture()
        with closing(sqlite3.connect(self.path)) as conn:
            before = conn.execute('SELECT * FROM bible_verses ORDER BY id').fetchall()
        self.bible._write_sync_status(self.path, dict(status='running', message='Indexing study dictionaries.'))
        lock = self.bible._sync_lock(self.path, create=True)
        self.assertIsNotNone(lock)
        try:
            status = self.client.get(self.base + '/api/status').json
            self.assertTrue(status['schema_ready'])
            self.assertGreater(status['indexed_verses'], 0)
            translations = self.client.get(self.base + '/api/translations').json['translations']
            self.assertTrue(next(item for item in translations if item['code'] == 'NIV')['available'])
            chapter = self.client.get(self.base + '/api/chapter').json
            self.assertTrue(chapter['ok'])
            self.assertTrue(chapter['verses'])
            sync = self.client.get(self.base + '/api/sync').json['sync']
            self.assertEqual(sync, dict(status='running', message='Indexing study dictionaries.'))
            self.assertEqual(self.client.post(self.base + '/api/sync', json={}, headers=headers).status_code, 409)
            self.assertIsNotNone(self.bible.get_indexed_niv_passage('Genesis 1:1', self.path))
        finally:
            self.bible._release_sync_lock(lock)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0], 7)
            self.assertEqual(conn.execute('SELECT * FROM bible_verses ORDER BY id').fetchall(), before)
            self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE name='bible_commentary_versions'").fetchone())

    def test_version_seven_explicit_upgrade_retains_verses_preferences_and_backup(self):
        self.version_seven_fixture()
        with closing(sqlite3.connect(self.path)) as conn:
            verses = conn.execute('SELECT * FROM bible_verses ORDER BY id').fetchall()
            state = conn.execute('SELECT * FROM bible_reader_state').fetchall()
        report = self.bible._safe_publish_sources(self.path, {}, {}, 'fixture_folder_id', [], lambda _: None)
        self.assertEqual(report['temporary_validation'], 'passed')
        self.assertTrue(Path(report['backup']).is_file())
        with closing(sqlite3.connect(report['backup'])) as backup:
            self.assertEqual(backup.execute('PRAGMA user_version').fetchone()[0], 7)
            self.assertEqual(backup.execute('SELECT * FROM bible_verses ORDER BY id').fetchall(), verses)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0], self.bible.SCHEMA_VERSION)
            self.assertEqual(conn.execute('SELECT * FROM bible_verses ORDER BY id').fetchall(), verses)
            self.assertEqual(conn.execute('SELECT * FROM bible_reader_state').fetchall(), state)
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_sync_progress_survives_reopening_and_does_not_need_database_reads(self):
        self.preview(initialize=True)
        lock = self.bible._sync_lock(self.path, create=True)
        try:
            progress = self.bible._SyncProgress(self.path)
            progress.update('Downloading study source.', stage='downloading', source='Dictionary.epub',
                            completed=128, total=256, unit='bytes', sources_done=3, sources_total=55)
            # A new browser session receives the same server job and counters.
            self.client = self.app.test_client()
            self.login()
            with patch.object(self.bible.sqlite3, 'connect', side_effect=AssertionError('Progress must not wait on SQLite')):
                result = self.client.get(self.base + '/api/sync').json['sync']
            self.assertEqual(result['status'], 'running')
            self.assertEqual(result['job_id'], progress.report['job_id'])
            self.assertEqual(result['progress']['completed'], 128)
            self.assertEqual(result['progress']['total'], 256)
            self.assertEqual(result['progress']['sources_done'], 3)
            progress.update('Publishing validated data.', stage='publishing')
            result = self.client.get(self.base + '/api/sync').json['sync']
            self.assertIsNone(result['progress']['total'])
            self.assertIsNone(result['progress']['completed'])
            self.assertEqual(result['progress']['sources_done'], 3)
        finally:
            self.bible._release_sync_lock(lock)
        result = self.client.get(self.base + '/api/sync').json['sync']
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['progress']['sources_done'], 3)

    def test_sync_notice_reaches_all_signed_in_roles_without_exposing_private_progress(self):
        self.preview(initialize=True)
        progress = self.bible._SyncProgress(self.path)
        progress.update('Private operator detail.', stage='indexing', source='private filename.epub',
                        completed=17, total=23, unit='sections')
        lock = self.bible._sync_lock(self.path, create=True)
        try:
            for account in (
                dict(ao_logged_in=True,ao_username='OtherAO',ao_role='Area Overseer'),
                dict(pastor_logged_in=True,pastor_username='Pastor'),
                dict(member_logged_in=True,username='Member'),
            ):
                with self.client.session_transaction() as saved:
                    saved.clear(); saved.update(account)
                response = self.client.get(self.base + '/api/sync-notice')
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.json['active'])
                self.assertEqual(set(response.json), {'ok','active','message'})
                self.assertNotIn('private filename',response.get_data(as_text=True))
                self.assertEqual(response.headers['Cache-Control'],'private, no-store')
                self.assertIn(self.client.get(self.base + '/api/sync').status_code, (401,403))
            with self.client.session_transaction() as saved:saved.clear()
            self.assertEqual(self.client.get(self.base+'/api/sync-notice').status_code,401)
        finally:
            self.bible._release_sync_lock(lock)
        self.login()
        self.assertFalse(self.client.get(self.base+'/api/sync-notice').json['active'])

    def test_download_progress_counts_actual_received_bytes_and_missing_totals(self):
        self.preview(initialize=True)
        response=Mock()
        response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        response.iter_content.return_value=[b'ab',b'cde']
        drive=Mock();drive.get.return_value=response
        updates=[]
        progress=Mock();progress.update.side_effect=lambda message,**details:updates.append(details)
        data=self.bible._download_source(drive,dict(drive_file_id='fixture_source_file',name='Study.epub'),progress)
        self.assertEqual(data,b'abcde')
        self.assertEqual([item['completed'] for item in updates],[0,2,5,5])
        self.assertTrue(all(item['total'] is None for item in updates[:-1]))
        self.assertEqual(updates[-1]['total'],5)

    def test_failed_sync_retains_last_measured_progress(self):
        self.preview(initialize=True)
        progress=self.bible._SyncProgress(self.path)
        progress.update('Indexing.',stage='indexing',completed=12,total=30,unit='sections')
        progress.finish(dict(status='failed',message='Source validation failed. Existing data was kept.'))
        report=self.client.get(self.base+'/api/sync').json['sync']
        self.assertEqual(report['status'],'failed')
        self.assertEqual(report['progress']['completed'],12)
        self.assertEqual(report['progress']['total'],30)

    def test_explicit_schema_contains_only_metadata_and_preserves_it_on_repeat(self):
        self.preview(initialize=True)
        conn = sqlite3.connect(self.path)
        try:
            self.assertEqual(conn.execute('PRAGMA application_id').fetchone()[0], self.bible.APPLICATION_ID)
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0], self.bible.SCHEMA_VERSION)
            self.assertEqual(conn.execute('SELECT COUNT(*),SUM(chapter_count) FROM bible_books').fetchone(), (66, 1189))
            tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            for table in tables:
                self.assertTrue(table.startswith('bible_'))
                if table not in ('bible_books', 'bible_metadata'):
                    self.assertEqual(conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 0)
            self.assertIn('bible_annotations', tables)
            self.assertIn('bible_bookmarks', tables)
            self.assertIn('bible_red_letter_spans', tables)
        finally:
            conn.close()
        self.app.bible_study_init_schema()
        self.assertEqual(self.client.get(self.base + '/api/status').json['status'], 'schema_ready')

    def test_initialization_refuses_existing_website_or_unrecognized_databases(self):
        for name in ('app_v2.db', 'ai_index.db', 'pastor_resource_sync_state.db'):
            target = Path(self.directory.name) / name
            with self.assertRaises(ValueError):
                self.bible.initialize_database(target)
            self.assertFalse(target.exists())
        target = Path(self.directory.name) / 'unrelated.db'
        conn = sqlite3.connect(target)
        conn.execute('CREATE TABLE unrelated (value TEXT)')
        conn.execute("INSERT INTO unrelated VALUES ('preserve')")
        conn.commit()
        conn.close()
        before = target.read_bytes()
        with self.assertRaises(ValueError):
            self.bible.initialize_database(target)
        self.assertEqual(before, target.read_bytes())
        with self.assertRaises(ValueError):
            self.bible.initialize_database('relative.db')
        with self.assertRaises(ValueError):
            self.bible.initialize_database(Path(resources.__file__).parent / 'static' / 'bible_study.db')

    def test_failed_schema_creation_rolls_back_all_tables(self):
        with patch.object(self.bible, 'SCHEMA_SQL', self.bible.SCHEMA_SQL + '; INVALID SQL'):
            with self.assertRaises(sqlite3.DatabaseError):
                self.app.bible_study_init_schema()
        conn = sqlite3.connect(self.path)
        try:
            self.assertEqual(conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [])
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0], 0)
        finally:
            conn.close()

    def test_csrf_payload_and_input_validation_happen_before_database_access(self):
        headers = self.preview()
        endpoint = self.base + '/api/state'
        self.assertEqual(self.client.post(endpoint, json=self.settings()).status_code, 400)
        self.assertEqual(self.client.post(endpoint, json=self.settings(), headers={'X-CSRF-Token': 'wrong'}).status_code, 400)
        self.assertEqual(self.client.post(endpoint, data='{}', headers=headers).status_code, 415)
        self.assertEqual(self.client.post(endpoint, data=' ' * 17000, content_type='application/json', headers=headers).status_code, 413)
        for data in ([], {'user_key': 'another-account'}, {**self.settings(), 'chapter': 51},
                     {**self.settings(), 'book': 'Jude', 'chapter': 2}, {**self.settings(), 'chapter': True},
                     {**self.settings(), 'theme': '<script>'}, {**self.settings(), 'font_size': 100}):
            with self.subTest(data=data):
                self.assertEqual(self.client.post(endpoint, json=data, headers=headers).status_code, 400)
        self.assertEqual(self.client.post(endpoint, json=self.settings(), headers=headers).status_code, 503)
        self.assertFalse(self.path.exists())

    def test_preferences_are_account_scoped_and_stale_writes_are_rejected(self):
        headers = self.preview(initialize=True)
        endpoint = self.base + '/api/state'
        data = {**self.settings(), 'book': 'John', 'chapter': 3, 'font_size': 22}
        self.assertEqual(self.client.post(endpoint, json=data, headers=headers).json['version'], 1)
        self.assertEqual(self.client.post(endpoint, json=data, headers=headers).status_code, 409)
        self.app.bible_study_init_schema()
        self.assertEqual(self.client.get(endpoint).json['state']['chapter'], 3)
        self.login('Pastor Two', pastor=True)
        self.assertEqual(self.client.get(endpoint + '?user_key=area%20overseer:pijeme').json['state']['version'], 0)
        self.assertEqual(self.client.get(endpoint).json['state']['book'], 'Genesis')
        self.login()
        self.assertEqual(self.client.get(endpoint).json['state']['book'], 'John')

    def test_concurrent_first_saves_do_not_overwrite_each_other(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        self.preview(initialize=True)
        clients = [self.app.test_client(), self.app.test_client()]
        headers = []
        for client in clients:
            with client.session_transaction() as account:
                account.update(ao_logged_in=True, ao_username='Pijeme', ao_role='Area Overseer')
            client.get(self.base)
            with client.session_transaction() as account:
                headers.append({'X-CSRF-Token': account['bible_study_csrf']})
        barrier = Barrier(2)
        def save(index):
            barrier.wait(timeout=5)
            return clients[index].post(self.base + '/api/state',
                json={**self.settings(), 'font_size': 20 + index}, headers=headers[index]).status_code
        with ThreadPoolExecutor(max_workers=2) as executor:
            self.assertEqual(sorted(executor.map(save, range(2))), [200, 409])
        self.assertEqual(self.client.get(self.base + '/api/state').json['state']['version'], 1)

    def test_schema_retains_personal_records_and_separates_lookup_evidence(self):
        self.preview(initialize=True)
        conn = sqlite3.connect(self.path)
        try:
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute("INSERT INTO bible_sources(id,title,sha256,source_path,created_at) VALUES(1,'Synthetic test source',?,'private/fixture.epub','now')", ('a'*64,))
            conn.execute("INSERT INTO bible_import_runs(id,source_id,status,started_at) VALUES(1,1,'staged','now')")
            for edition_id, translation in ((1,'NIV'), (2,'KJV')):
                conn.execute("INSERT INTO bible_editions(id,source_id,import_run_id,edition_key,translation,title) VALUES(?,1,1,?,?,'Synthetic fixture')",
                             (edition_id, translation, translation))
                conn.execute("INSERT INTO bible_verses(id,edition_id,book_id,chapter,verse,text,content_hash) VALUES(?,?,1,1,1,'Synthetic fixture text.',?)",
                             (edition_id, edition_id, 'b'*64))
            conn.execute("INSERT INTO bible_tokens(id,verse_id,token_index,start_offset,end_offset,text) VALUES(1,1,0,0,9,'Synthetic')")
            conn.execute("INSERT INTO bible_lexicon(id,source_id,strong_id,language,definition,source_anchor) VALUES(1,1,'H1','Hebrew','Fixture only','dictionary#1')")
            candidate = (1,1,'kjv_based_lookup',0,'kjv_verse_lookup',1,2,'Synthetic','kjv#1','Synthetic source evidence')
            sql = '''INSERT INTO bible_word_links(token_id,lexicon_id,result_type,verified,match_method,
                evidence_source_id,evidence_verse_id,source_word,evidence_locator,evidence_detail)
                VALUES(?,?,?,?,?,?,?,?,?,?)'''
            conn.execute(sql, candidate)
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(sql, (1,1,'verified',1,'kjv_verse_lookup',1,2,'Synthetic','kjv#1','Not an exact mapping'))
            conn.execute("""INSERT INTO bible_annotations(id,user_key,verse_id,group_id,kind,start_offset,end_offset,selected_text,note,created_at,updated_at)
                VALUES('annotation','pastor:fixture',1,'group','highlight',0,9,'Synthetic','Keep this note','now','now')""")
            conn.execute("INSERT INTO bible_bookmarks(id,user_key,verse_id,created_at,updated_at) VALUES('bookmark','pastor:fixture',1,'now','now')")
            conn.commit()
            before = (conn.execute('SELECT * FROM bible_annotations').fetchall(), conn.execute('SELECT * FROM bible_bookmarks').fetchall())
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute('DELETE FROM bible_verses WHERE id=1')
            conn.rollback()
        finally:
            conn.close()
        self.app.bible_study_init_schema()
        conn = sqlite3.connect(self.path)
        try:
            after = (conn.execute('SELECT * FROM bible_annotations').fetchall(), conn.execute('SELECT * FROM bible_bookmarks').fetchall())
            self.assertEqual(after, before)
        finally:
            conn.close()
        self.assertEqual(self.client.get(self.base + '/api/status').json['indexed_verses'], 0)

    def test_security_fallback_and_anonymous_access_fail_closed(self):
        self.assertEqual(self.client.get(self.base).status_code, 302)
        self.assertEqual(self.client.get(self.base + '/api/state').status_code, 401)
        self.app.secret_key = 'change-this-secret-key-123'
        self.login()
        self.assertEqual(self.client.get(self.base + '/api/status').status_code, 503)
        self.assertFalse(self.path.exists())

    def fixture_epub(self, documents, assets=None, metadata=''):
        import io
        import zipfile
        output = io.BytesIO()
        with zipfile.ZipFile(output, 'w') as archive:
            archive.writestr('META-INF/container.xml', '<container><rootfiles><rootfile full-path="OEBPS/content.opf"/></rootfiles></container>')
            manifest, spine = [], []
            for index, (name, body) in enumerate(documents):
                manifest.append(f'<item id="p{index}" href="{name}"/>')
                spine.append(f'<itemref idref="p{index}"/>')
                archive.writestr('OEBPS/' + name, '<html xmlns="http://www.w3.org/1999/xhtml"><body>' + body + '</body></html>')
            archive.writestr('OEBPS/content.opf', '<package><metadata>'+metadata+'</metadata><manifest>' + ''.join(manifest) + '</manifest><spine>' + ''.join(spine) + '</spine></package>')
            for name, data in (assets or {}).items():
                archive.writestr('OEBPS/' + name, data)
        return output.getvalue()

    def dictionary_fixtures(self):
        import io
        from PIL import Image
        image = io.BytesIO()
        Image.new('RGB', (3, 3), 'black').save(image, format='GIF')
        mounce = self.fixture_epub([
            ('main.html', '<p class="h2" id="ch5">ALPHA</p><p class="h1">Old Testament</p><p class="tx"><b>Noun:</b> <img src="glyph.gif"/> (<i>alpha</i>), GK 10 (S 20), 2x. Synthetic definition. Gen. 1:1.</p>'),
            ('hebrew.html', '<p class="h" id="ch6">Hebrew-English Dictionary</p><p class="tx">[99] Introductory example, not an entry.</p><p class="tx2"><b>[10]</b> <img src="glyph.gif"/> <i>alpha</i> 2x synthetic Hebrew entry [20*]</p>'),
            ('greek.html', '<p class="h" id="ch7">Greek-English Dictionary</p><p class="tx2"><b>[10]</b> <img src="glyph.gif"/> <i>alpha</i> 2x synthetic Greek entry [20]</p><p class="toch">About the Publisher</p><p>Do not append marketing text.</p>'),
        ], {'glyph.gif': image.getvalue()})
        vine = self.fixture_epub([
            ('vine-otA.html', '<div class="chap" id="ALPHA"><p class="bh">ALPHA</p><p><i>alpha</i> (20*) &ldquo;Synthetic Hebrew definition.&rdquo; Gen. 1:1.</p></div>'),
            ('vine-ntA.html', '<div class="chap" id="G.ALPHA"><p class="bh">ALPHA</p><p><i>alpha</i> (20) Synthetic Greek definition.</p></div>'),
            ('vine-ntAa.html', '<div class="chap"><p>Continued original entry. John 3:16; 18:2.</p></div><div class="chap" id="G.BETA"><p class="bh">BETA</p><p>A count (365) must not become a Strong’s identifier.</p></div>'),
        ])
        return {'mounce': mounce, 'vine': vine}

    def publish_dictionaries(self):
        data = self.dictionary_fixtures()
        selected = {family: dict(title=family.title()+' fixture', drive_file_id='fixture_'+family+'_id',
                                sha256=('c' if family=='mounce' else 'd')*64, fingerprint={'modifiedTime':'now'})
                    for family in data}
        parsed = {family: self.bible.parse_dictionary_epub(content, family) for family, content in data.items()}
        report = self.bible._publish_dictionary_sync(self.path, selected, parsed, 'fixture_folder_id', [])
        return selected, parsed, report

    def publish_fixture(self, changed=False):
        sources = {
            'NIV': dict(title='Synthetic NIV fixture', drive_file_id='fixture_niv_id', sha256=('c' if changed else 'a') * 64),
            'KJV': dict(title='Synthetic KJV fixture', drive_file_id='fixture_kjv_id', sha256='b' * 64),
        }
        parsed = {
            'NIV': dict(verses={(1, 1, 1): dict(text='Alpha 😀 beta.' + (' Revised.' if changed else ''), heading='Fixture heading')},
                        lexicon={}, links=[]),
            'KJV': dict(verses={(1, 1, 1): dict(text='Alpha beta.', heading='')},
                        lexicon={'dictionary.html#one': dict(strong_id='H1', language='Hebrew', pronunciation='fixture', definition='Synthetic definition.')},
                        links=[((1, 1, 1), 'dictionary.html#one', 'Alpha', 'kjv.html#verse1')]),
        }
        return sources, parsed, self.bible._publish_sources(self.path, sources, parsed)

    def illustration_fixture(self):
        archive=Path(self.directory.name)/'illustrations.zip'
        raw=Path(self.directory.name)/'illustrations-input.db'
        with closing(sqlite3.connect(raw)) as conn,conn:
            for table,definition in self.bible.ILLUSTRATION_TABLES.items():conn.execute('CREATE TABLE '+table+' ('+definition+')')
            for number in range(45):
                identifier=f'i-{number:03d}';source=list(self.bible.ILLUSTRATION_SOURCES)[number%3]
                body='' if number==5 else ('Shared story.' if number in (0,2) else f'Faith story number {number}.')
                html='<p><strong>'+body+'</strong> <u>Underlined</u><br>Second line.</p><script>window.injected=true</script>' if body else ''
                conn.execute('INSERT INTO illustrations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                    (identifier,source,number,'Illustration '+str(number),'Faith','["Faith"]','[]',html,body,'Contributor','https://example.test/story','[]'))
                conn.execute('INSERT INTO illustration_topics VALUES(?,?)',(identifier,'Faith'))
                conn.execute('INSERT INTO browse_topic_illustrations VALUES(?,?,?)',('faith',identifier,'Illustration '+str(number)))
                original=number==0;kind='original' if original else 'suggested';confidence='source' if original else 'low' if number==3 else 'medium' if number>30 else 'high'
                refs=[] if number in (4,5) else [(identifier,'Ephesians',5,6 if number==1 else 5,21 if number==1 else 1,4 if number==1 else 1,
                    'Ephesians 5:21–6:4' if number==1 else 'Ephesians 5:1',kind,1,'Faith',confidence,'fixture','needs_review')]
                if number==4:refs=[(identifier,'Isaiah',0,0,12,14,'Isaiah 0:12–14','original',1,None,'source','fixture','original_not_reviewed')]
                conn.executemany('INSERT INTO scripture_verse_links VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',refs)
                conn.execute('INSERT INTO annotation_summary VALUES(?,?,?,?,?,?)',(identifier,int(original or number==4),int(bool(refs) and not original and number!=4),confidence,'Faith',1))
            conn.execute('INSERT INTO browse_topics VALUES(?,?,?,?,?)',('faith','Faith','F',45,3))
            # Imported SQL/triggers must never become executable app schema.
            conn.execute("CREATE TRIGGER untrusted_trigger AFTER DELETE ON illustrations BEGIN DELETE FROM browse_topics; END")
        with zipfile.ZipFile(archive,'w') as zipped:
            zipped.write(raw,'sermon_illustrations_scripture_enriched.sqlite3')
            zipped.writestr('SAMPLE_QUERIES.sql','DROP TABLE illustrations;')
        return archive

    def test_illustration_import_preserves_text_browsing_and_rolls_back_bad_packages(self):
        import hashlib
        archive=self.illustration_fixture();path=Path(self.directory.name)/'sermon_illustrations.db'
        report=self.bible.import_illustration_archive(path,archive,expected_count=45)
        self.assertEqual(report['illustration_entries'],45);self.assertEqual(report['invalid_references'],1)
        with self.bible.illustration_connection(path) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM illustrations').fetchone()[0],45)
            self.assertEqual(conn.execute('SELECT COUNT(DISTINCT illustration_id) FROM browse_topic_illustrations').fetchone()[0],45)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'").fetchone()[0],0)
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(),[])
            article=self.bible.illustration_article(conn,'i-000')
            self.assertIn('Shared story.',article['body_text']);self.assertNotIn('window.injected',str(article['blocks']))
            self.assertTrue(any('u' in r['marks'] for b in article['blocks'] for r in b['runs']))
            self.assertTrue(self.bible.illustration_article(conn,'i-005')['needs_source_review'])
            bad=self.bible.illustration_article(conn,'i-004')
            self.assertFalse(bad['references']);self.assertEqual(bad['reference_issues'][0]['display_reference'],'Isaiah 0:12–14')
            page=self.bible.illustration_browse(conn,letter='F',topic='faith')
            self.assertEqual(len(page['entries']),30);self.assertEqual(page['next_offset'],30)
            ids={e['id'] for e in page['entries']}
            ids.update(e['id'] for e in self.bible.illustration_browse(conn,topic='faith',offset=30)['entries'])
            self.assertEqual(len(ids),45);self.assertIn('i-005',ids)
            self.assertTrue(self.bible.illustration_browse(conn,query='Faith')['entries'])
            self.assertFalse(self.bible.illustration_browse(conn,query='" OR *')['entries'])
        self.assertEqual(self.bible.import_illustration_archive(path,archive,expected_count=45)['status'],'unchanged')
        before=hashlib.sha256(path.read_bytes()).hexdigest()
        with zipfile.ZipFile(archive,'a') as zipped:zipped.writestr('note.txt','changed archive')
        with self.assertRaises(self.bible.SyncError):self.bible.import_illustration_archive(path,archive,expected_count=46)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),before)
        with self.assertRaises(ValueError):self.bible.illustration_database_path(self.path,self.path)

    def test_illustration_suggestions_rank_deduplicate_and_bound_cross_chapter_ranges(self):
        archive=self.illustration_fixture();path=Path(self.directory.name)/'sermon_illustrations.db'
        self.bible.import_illustration_archive(path,archive,expected_count=45)
        with self.bible.illustration_connection(path) as conn:
            start=self.bible.illustration_suggestions(conn,49,5,1)
            self.assertEqual(len(start),30);self.assertEqual(start[0]['id'],'i-000')
            self.assertEqual(start[0]['match']['link_type'],'original')
            self.assertFalse({'i-001','i-002','i-003','i-004','i-005'} & {e['id'] for e in start})
            for chapter,verse,expected in [(5,20,False),(5,21,True),(5,33,True),(6,1,True),(6,4,True),(6,5,False)]:
                self.assertEqual(any(e['id']=='i-001' for e in self.bible.illustration_suggestions(conn,49,chapter,verse)),expected)
            self.assertTrue(any(e['id']=='i-001' for e in self.bible.illustration_suggestions(conn,49,5,20,22)))
            self.assertTrue(any(e['id']=='i-001' for e in self.bible.illustration_suggestions(conn,49,6)))
            self.assertFalse(self.bible.illustration_suggestions(conn,23,54,11))

    def test_illustration_routes_are_private_read_only_and_validate_inputs(self):
        archive=self.illustration_fixture();path=Path(self.directory.name)/'sermon_illustrations.db'
        self.app.config['BIBLE_ILLUSTRATIONS_DATABASE']=str(path)
        self.assertEqual(self.client.get(self.base+'/api/illustrations/browse').status_code,401)
        self.login()
        self.assertFalse(self.client.get(self.base+'/api/illustrations/browse').json['available'])
        self.assertFalse(path.exists());self.assertFalse(self.path.exists())
        self.bible.import_illustration_archive(path,archive,expected_count=45)
        response=self.client.get(self.base+'/api/illustrations/suggested?book=Ephesians&chapter=5&verse=1')
        self.assertTrue(response.json['available']);self.assertEqual(len(response.json['entries']),30)
        self.assertIn('no-store',response.headers['Cache-Control'])
        self.assertEqual(self.client.get(self.base+'/api/illustrations/browse?topic=faith').json['next_offset'],30)
        self.assertEqual(self.client.get(self.base+'/api/illustrations/entry/i-000').json['illustration']['source_credit'],'Contributor')
        self.assertEqual(self.client.get(self.base+'/api/illustrations/entry/missing').status_code,404)
        for suffix in ['/suggested?book=Isaiah&chapter=0','/suggested?book=John&chapter=3&verse=abc',
                '/suggested?book=John&chapter=3&verse=16&end_verse=1','/browse?offset=-1','/browse?source=bad']:
            self.assertEqual(self.client.get(self.base+'/api/illustrations'+suffix).status_code,400)
        self.assertFalse(self.path.exists())

    def test_illustration_owner_approval_persists_and_suggestions_include_all_matching_collections(self):
        archive=self.illustration_fixture();path=Path(self.directory.name)/'sermon_illustrations.db'
        self.bible.import_illustration_archive(path,archive,expected_count=45)
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute("DELETE FROM illustration_metadata WHERE key='review_policy'")
            conn.execute("UPDATE scripture_verse_links SET review_status='needs_review'")
            conn.execute("UPDATE illustration_valid_links SET review_status='needs_review'")
            conn.execute('UPDATE annotation_summary SET needs_human_review=1')
            # Isolate one low-confidence connection to verify owner approval
            # makes it available without changing its original confidence.
            conn.execute("UPDATE illustration_valid_links SET chapter_start=7,chapter_end=7,verse_start=1,verse_end=1 WHERE illustration_id='i-003'")
            text=conn.execute('SELECT id,body_html,body_text FROM illustrations ORDER BY id').fetchall()
            confidence=conn.execute('SELECT illustration_id,confidence FROM scripture_verse_links ORDER BY illustration_id').fetchall()
        with self.bible.illustration_connection(path) as conn:
            self.assertFalse(self.bible.illustration_suggestions(conn,49,7,1))
        report=self.bible.approve_illustration_reviews(path,'Owner test request')
        self.assertEqual(report['approved_articles'],45);self.assertTrue(Path(report['backup']).is_file())
        with self.bible.illustration_connection(path) as conn:
            self.assertEqual([tuple(r) for r in conn.execute('SELECT id,body_html,body_text FROM illustrations ORDER BY id')],text)
            self.assertEqual([tuple(r) for r in conn.execute('SELECT illustration_id,confidence FROM scripture_verse_links ORDER BY illustration_id')],confidence)
            self.assertEqual(conn.execute('SELECT SUM(needs_human_review) FROM annotation_summary').fetchone()[0],0)
            self.assertEqual(self.bible.illustration_suggestions(conn,49,7,1)[0]['id'],'i-003')
            suggestions=self.bible.illustration_suggestions(conn,49,5,1)
            self.assertEqual(len(suggestions),30)
            self.assertEqual([s['source'] for s in suggestions[:5]],['connect_1001']*5)
            self.assertTrue(all(s['source']!='connect_1001' for s in suggestions[5:10]))
            self.assertTrue(conn.execute('SELECT 1 FROM illustration_reference_issues').fetchone())
        self.assertEqual(self.bible.approve_illustration_reviews(path)['status'],'unchanged')
        with zipfile.ZipFile(archive,'a') as zipped:zipped.writestr('policy-repeat.txt','New package fingerprint')
        self.bible.import_illustration_archive(path,archive,expected_count=45)
        with self.bible.illustration_connection(path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM illustration_valid_links WHERE review_status!='approved_for_use'").fetchone()[0],0)
            self.assertEqual(conn.execute('SELECT SUM(needs_human_review) FROM annotation_summary').fetchone()[0],0)

    def test_illustration_drive_sync_is_repeatable_and_never_changes_the_ai_index(self):
        archive=self.illustration_fixture();content=archive.read_bytes()
        ai=Path(self.directory.name)/'ai.db';ai.write_bytes(b'AI index sentinel')
        source=dict(name='sermon_illustrations_codex_v2.zip',drive_file_id='fixture_illustration_id',
            fingerprint=dict(modifiedTime='now',sha256Checksum=hashlib.sha256(content).hexdigest()))
        importer=self.bible.import_illustration_archive
        with patch.object(self.bible,'_official_drive_sources',return_value=('fixture_folder_id',{'illustrations':source},[])), \
             patch.object(self.bible,'import_illustration_archive',side_effect=lambda path,data,**options:importer(path,data,expected_count=45,**options)), \
             patch.object(self.bible,'purge_migrated_ai_sources',side_effect=AssertionError('Must not modify AI index')):
            report=self.bible.sync_sources(self.path,'fixture_folder_id',lambda _:None,fetch=lambda _:content,drive=Mock(),ai_database=ai)
        self.assertEqual(report['counts']['illustration_entries'],45)
        self.assertEqual(ai.read_bytes(),b'AI index sentinel');self.assertFalse(self.path.exists())
        with patch.object(self.bible,'_official_drive_sources',return_value=('fixture_folder_id',{'illustrations':source},[])):
            self.bible.sync_sources(self.path,'fixture_folder_id',lambda _:None,
                fetch=lambda _:self.fail('Unchanged illustration ZIP was downloaded again'),drive=Mock())
        with patch.object(resources,'list_drive_folder',return_value=[
                dict(id='fixture_v2_file',name='sermon_illustrations_codex_v2.zip'),
                dict(id='fixture_old_file',name='sermon_illustrations_full_import.zip')]):
            _,selected,ignored=self.bible._official_drive_sources(Mock(),'fixture_folder_id')
        self.assertEqual(set(selected),{'illustrations'});self.assertIn('sermon_illustrations_full_import.zip',ignored)

    def test_illustration_first_page_is_five_plus_five_then_global_ranking_without_source_quotas(self):
        archive=self.illustration_fixture();path=Path(self.directory.name)/'sermon_illustrations.db'
        self.bible.import_illustration_archive(path,archive,expected_count=45)
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute("UPDATE illustration_valid_links SET link_type='suggested',confidence='high',priority=10")
            conn.execute("UPDATE illustration_valid_links SET priority=1 WHERE illustration_id IN (SELECT id FROM illustrations WHERE source='sermonillustrations_web')")
        with self.bible.illustration_connection(path) as conn:
            rows=conn.execute('''SELECT i.id,i.title,i.source,l.priority FROM illustration_valid_links l
                JOIN illustrations i ON i.id=l.illustration_id JOIN illustration_reading r ON r.id=i.id
                WHERE chapter_start=5 AND chapter_end=5 AND verse_start=1 AND has_content=1
                ORDER BY priority,i.title COLLATE NOCASE,i.id''').fetchall()
            # Cross-source duplicate body uses the stronger matching record.
            self.assertIn('i-002',[r['id'] for r in rows])
            expected=[r for r in rows if r['id']!='i-000']
            first1001=[r['id'] for r in expected if r['source']=='connect_1001'][:5]
            first_other=[r['id'] for r in expected if r['source']!='connect_1001'][:5]
            selected=set(first1001+first_other)
            remainder=[r['id'] for r in expected if r['id'] not in selected][:20]
            result=self.bible.illustration_suggestions(conn,49,5,1)
            self.assertEqual([r['id'] for r in result],first1001+first_other+remainder)
            self.assertEqual({r['source'] for r in result[5:10]},{'sermonillustrations_web'})
            self.assertTrue(any(r['source']=='connect_1001' for r in result[10:]))
            self.assertEqual(len({r['id'] for r in result}),30)
            # With fewer than five matches in one side, use available matches;
            # no filler articles or duplicated cards are created.
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute("DELETE FROM illustration_valid_links WHERE illustration_id NOT IN ('i-003','i-007','i-008')")
        with self.bible.illustration_connection(path) as conn:
            result=self.bible.illustration_suggestions(conn,49,5,1)
            self.assertEqual(len(result),3);self.assertEqual(result[0]['id'],'i-003')

    def test_epub_parsers_preserve_references_and_supported_word_link_evidence(self):
        niv = self.fixture_epub([('niv.html', '<p class="ah">Fixture heading</p><p id="xhtmlv01001001"><sup>1</sup>Alpha 😀 <a href="notes.html#xhtmlfn01001" id="xhtmlrfn01001">ignore</a>beta.<a id="xhtmlv01001002"/><sup>2</sup>Gamma.</p>')])
        parsed = self.bible.parse_niv_epub(niv)
        self.assertEqual(parsed['verses'][(1, 1, 1)]['text'], 'Alpha 😀 beta.')
        self.assertEqual(parsed['verses'][(1, 1, 2)]['text'], 'Gamma.')
        suite = self.fixture_epub([
            ('kjv.html', '<p><span id="verse1"/><font color="green">Gen 1:1</font>Alpha<a href="dictionary.html#one"><img src="icon.jpg"/></a> beta.</p>'),
            ('dictionary.html', '<h1>HEBREW AND CHALDEE DICTIONARY OF THE OLD TESTAMENT</h1><p><span id="one"/></p><p>{fixture} Strong\'s {01}<b>Definitions:</b> Synthetic definition.</p>'),
        ])
        parsed = self.bible.parse_suite_epub(suite)
        self.assertEqual(parsed['verses'][(1, 1, 1)]['text'], 'Alpha beta.')
        self.assertEqual(parsed['lexicon']['OEBPS/dictionary.html#one']['strong_id'], 'H1')
        self.assertEqual(parsed['links'], [((1, 1, 1), 'OEBPS/dictionary.html#one', 'Alpha', 'OEBPS/kjv.html#verse1')])
        with self.assertRaises(self.bible.SyncError):
            self.bible.validate_sources(self.bible.parse_niv_epub(niv), parsed)
        with self.assertRaises(self.bible.SyncError):
            list(self.bible._epub_documents(b'not an EPUB'))
        with self.assertRaises(self.bible.SyncError):
            self.bible._xml(b'<!DOCTYPE a [<!ENTITY x "bad">]><a>&x;</a>')
        for href in ('https://example.test/file', '../../escape', '/absolute', '..\\escape'):
            self.assertEqual(self.bible._epub_target('OEBPS/file.html', href), '')

    def test_repeat_sync_and_new_editions_preserve_personal_records_and_rollback(self):
        self.preview(initialize=True)
        sources, parsed, report = self.publish_fixture()
        self.assertEqual(report['status'], 'completed')
        with closing(sqlite3.connect(self.path)) as conn, conn:
            verse_id = conn.execute("SELECT v.id FROM bible_verses v JOIN bible_editions e ON e.id=v.edition_id WHERE e.translation='NIV'").fetchone()[0]
            conn.execute("""INSERT INTO bible_annotations(id,user_key,verse_id,group_id,kind,start_offset,end_offset,selected_text,note,created_at,updated_at)
                VALUES('keep','area overseer:pijeme',?,'g','note',0,5,'Alpha','Keep me','now','now')""", (verse_id,))
            conn.execute("INSERT INTO bible_bookmarks(id,user_key,verse_id,created_at,updated_at) VALUES('keep','area overseer:pijeme',?,'now','now')", (verse_id,))
            before = conn.execute('SELECT COUNT(*) FROM bible_verses').fetchone()[0]
        self.assertIn('unchanged', self.publish_fixture()[2]['message'])
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_verses').fetchone()[0], before)
        bad_sources = copy.deepcopy(sources)
        bad_sources['NIV']['sha256'] = 'd' * 64
        bad_sources['KJV']['sha256'] = 'e' * 64
        bad_parsed = copy.deepcopy(parsed)
        bad_parsed['KJV']['links'][0] = ((1,1,1), 'missing-entry', 'Alpha', 'bad')
        with self.assertRaises(KeyError):
            self.bible._publish_sources(self.path, bad_sources, bad_parsed)
        missing_verses = copy.deepcopy(parsed)
        missing_verses['NIV']['verses'] = {}
        with self.assertRaises(self.bible.SyncError):
            self.bible._publish_sources(self.path, bad_sources, missing_verses)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_verses').fetchone()[0], before)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_editions WHERE is_active=1').fetchone()[0], 2)
        self.publish_fixture(changed=True)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT verse_id,note FROM bible_annotations').fetchone(), (verse_id, 'Keep me'))
            self.assertEqual(conn.execute('SELECT verse_id FROM bible_bookmarks').fetchone()[0], verse_id)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_verses').fetchone()[0], before + 1)
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertIn('Revised.', self.client.get(self.base + '/api/chapter').json['verses'][0]['text'])

    def test_sqlite_reader_removes_kjv_results_and_provides_complete_verse_previews(self):
        self.preview(initialize=True)
        self.publish_fixture()
        # Runtime reads never call Drive or open EPUBs.
        with patch.object(resources, 'get_drive_session', side_effect=AssertionError('Reader opened an EPUB')):
            chapter = self.client.get(self.base + '/api/chapter').json
            verse = chapter['verses'][0]
            self.assertEqual(verse['text'], 'Alpha 😀 beta.')
            self.assertEqual(verse['tokens'][1]['start_offset'], 8)
            match = self.client.get(f"{self.base}/api/word/{verse['id']}/0").json
            self.assertFalse(match['verified'])
            self.assertTrue(all(not group['available'] for group in match['sources']))
            self.assertEqual(match['message'], self.bible.NO_VERIFIED_ALIGNMENT)
            self.assertNotIn('KJV', str(match))
            self.assertEqual(self.client.get(f'{self.base}/api/concordance/1').status_code, 404)
            preview = self.client.get(self.base + '/api/verse?book=Genesis&chapter=1&verse=1').json['verse']
            self.assertEqual(preview['text'], verse['text'])
            self.assertEqual(preview['reference'], 'Genesis 1:1')
        self.assertEqual(self.client.get(self.base + '/api/verse?book=Genesis&chapter=1&verse=2').status_code, 404)
        self.assertEqual(self.client.get(self.base + '/api/chapter?book=Jude&chapter=2').status_code, 400)
        self.assertEqual(self.client.get(self.base + '/api/status').json['indexed_verses'], 1)

    def test_bible_saves_validate_unicode_anchors_and_are_private_csrf_protected_and_versioned(self):
        headers = self.preview(initialize=True)
        self.publish_fixture()
        verse = self.client.get(self.base + '/api/chapter').json['verses'][0]
        payload = dict(kind='highlight', color='#9ee6b8', note='', ranges=[
            dict(verse_id=verse['id'], start_offset=6, end_offset=12, selected_text='😀 beta')])
        self.assertEqual(self.client.post(self.base + '/api/annotations', json=payload).status_code, 400)
        wrong = {**payload, 'ranges': [{**payload['ranges'][0], 'start_offset': 7}]}
        self.assertEqual(self.client.post(self.base + '/api/annotations', json=wrong, headers=headers).status_code, 400)
        wrong = {**payload, 'color': 'url(javascript:alert(1))'}
        self.assertEqual(self.client.post(self.base + '/api/annotations', json=wrong, headers=headers).status_code, 400)
        response = self.client.post(self.base + '/api/annotations', json=payload, headers=headers)
        self.assertEqual(response.status_code, 201, response.json)
        item = response.json['annotation']; url = item['delete_url']
        self.assertEqual(item['selected_text'], '😀 beta')
        self.assertIn('annotation=', item['jump_url'])
        self.assertEqual(self.client.patch(url, json=dict(note='<script>private note</script>', version=1), headers=headers).status_code, 200)
        self.assertEqual(self.client.patch(url, json=dict(note='stale', version=1), headers=headers).status_code, 409)
        bookmark = self.client.post(self.base + '/api/bookmarks', json=dict(verse_id=verse['id']), headers=headers).json['bookmark']
        again = self.client.post(self.base + '/api/bookmarks', json=dict(verse_id=verse['id']), headers=headers).json['bookmark']
        self.assertEqual(bookmark['id'], again['id'])
        self.login('Other Pastor', pastor=True)
        self.client.get(self.base)
        with self.client.session_transaction() as account: other_headers = {'X-CSRF-Token': account['bible_study_csrf']}
        self.assertEqual(self.client.get(self.base + '/api/personal').json['annotations'], [])
        for method in ('get', 'delete', 'patch'):
            response = getattr(self.client, method)(url, json={} if method != 'get' else None, headers=other_headers)
            self.assertEqual(response.status_code, 404)
        self.login()
        headers = self.preview()
        saved = self.client.get(url).json['item']
        self.assertEqual(saved['note'], '<script>private note</script>')
        self.assertEqual(saved['version'], 2)
        self.assertEqual(self.client.delete(url, json={}, headers=headers).status_code, 200)
        self.assertEqual(self.client.get(url).status_code, 404)
        self.assertEqual(self.client.delete(bookmark['delete_url'], json={}, headers=headers).status_code, 200)

    def test_multi_verse_bible_annotations_merge_in_global_library_and_delete_as_one_group(self):
        headers = self.preview(initialize=True); self.publish_fixture()
        verse = self.client.get(self.base + '/api/chapter').json['verses'][0]
        with closing(sqlite3.connect(self.path)) as conn:
            edition = conn.execute('SELECT edition_id FROM bible_verses WHERE id=?', (verse['id'],)).fetchone()[0]
            cursor = conn.execute('''INSERT INTO bible_verses(edition_id,book_id,chapter,verse,text,content_hash)
                VALUES(?,1,1,2,'Gamma.',?)''', (edition, 'e' * 64))
            second = cursor.lastrowid
            conn.commit()
        spans = [dict(verse_id=verse['id'], start_offset=8, end_offset=13, selected_text='beta.'),
                 dict(verse_id=second, start_offset=0, end_offset=5, selected_text='Gamma')]
        result = self.client.post(self.base + '/api/annotations', json=dict(kind='note', color=None, note='My note', ranges=spans), headers=headers)
        self.assertEqual(result.status_code, 201, result.json)
        item = result.json['annotation']
        self.assertEqual(len(self.client.get(self.base + '/api/personal').json['annotations']), 1)
        self.assertEqual(item['reference'], 'Genesis 1:1–2')
        self.assertEqual(len(item['ranges']), 2)
        # Exercise the real global collection route with only ebook storage mocked.
        library = Flask('global_bible_saves'); library.secret_key = self.app.secret_key
        library.config['BIBLE_STUDY_DATABASE'] = str(self.path)
        with patch.object(resources, 'ensure_v3_tables'):
            resources.register_pastor_resources_routes(library)
        self.bible.register_bible_study_routes(library)
        client = library.test_client()
        with client.session_transaction() as account: account.update(ao_logged_in=True, ao_username='Pijeme', ao_role='Area Overseer')
        with patch.object(resources, 'any_user_logged_in', return_value=True), patch.object(resources, 'get_my_library_payload', return_value=dict(annotations=[], bookmarks=[], favorites=[], currently_reading=[], completed=[])):
            data = client.get('/pastor-resources/api/my-library').json
        self.assertEqual(data['annotations'][0]['source'], 'bible')
        self.assertEqual(data['annotations'][0]['id'], 'bible:' + item['id'])
        self.assertIn('annotation=', data['annotations'][0]['jump_url'])
        self.assertEqual(client.delete(data['annotations'][0]['delete_url'], json={}, headers={'X-CSRF-Token': data['bible_csrf_token']}).status_code, 200)
        with closing(sqlite3.connect(self.path)) as conn: self.assertEqual(conn.execute('SELECT count(*) FROM bible_annotations').fetchone()[0], 0)

    def test_saved_bible_edition_stays_identifiable_after_source_updates(self):
        headers = self.preview(initialize=True); self.publish_fixture()
        verse = self.client.get(self.base + '/api/chapter').json['verses'][0]
        saved = self.client.post(self.base + '/api/bookmarks', json=dict(verse_id=verse['id']), headers=headers).json['bookmark']
        self.publish_fixture(changed=True)
        item = self.client.get(saved['delete_url']).json['item']
        self.assertEqual(item['verse_id'], verse['id'])
        self.assertFalse(item['available'])
        self.assertEqual(self.client.post(self.base + '/api/bookmarks', json=dict(verse_id=verse['id']), headers=headers).status_code, 400)

    def test_daily_niv_passage_supports_psalm_alias_and_complete_ranges(self):
        self.preview(initialize=True)
        sources, parsed, _ = self.publish_fixture()
        self.assertEqual(self.bible.get_indexed_niv_passage('Genesis 1:1', self.path), 'Alpha 😀 beta.')
        self.assertIsNone(self.bible.get_indexed_niv_passage('Genesis 1:1-2', self.path))
        sources['NIV']['sha256'] = 'f' * 64
        parsed['NIV']['verses'].update({
            (19, 24, 3): dict(text='Synthetic third verse.', heading=''),
            (19, 24, 4): dict(text='Synthetic fourth verse.', heading=''),
        })
        self.bible._publish_sources(self.path, sources, parsed)
        expected = 'Synthetic third verse. Synthetic fourth verse.'
        self.assertEqual(self.bible.get_indexed_niv_passage('Psalm 24:3-4', self.path), expected)
        self.assertEqual(self.bible.get_indexed_niv_passage('Psalms 24:3–4', self.path), expected)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("UPDATE bible_editions SET status='staged' WHERE translation='NIV'")
        self.assertIsNone(self.bible.get_indexed_niv_passage('Genesis 1:1', self.path))

    def test_daily_niv_passage_missing_storage_and_invalid_references_do_not_create_data(self):
        with patch.object(self.bible.sqlite3, 'connect', side_effect=AssertionError('No database creation')):
            for reference in ('Genesis 1:1', 'Genesis 1:2-1', 'Genesis 51:1', 'Genesis 1:177',
                              'Unknown 1:1', 'Genesis 1:1 OR 1=1', 'Genesis 1:1; DROP TABLE verses', None):
                self.assertIsNone(self.bible.get_indexed_niv_passage(reference, self.path))
        self.assertFalse(self.path.exists())
        foreign_path = Path(self.directory.name) / 'foreign.db'
        with closing(sqlite3.connect(foreign_path)) as conn, conn:
            conn.execute('CREATE TABLE keep_me(value TEXT)')
        before = foreign_path.read_bytes()
        self.assertIsNone(self.bible.get_indexed_niv_passage('Genesis 1:1', foreign_path))
        self.assertEqual(foreign_path.read_bytes(), before)

    def test_bulletin_daily_verse_keeps_rotation_uses_local_date_and_bypasses_old_kjv_cache(self):
        # Execute the actual function AST without importing app.py, whose startup
        # hooks touch website databases and external services.
        import ast
        from datetime import datetime
        from zoneinfo import ZoneInfo
        tree = ast.parse((Path(resources.__file__).parent / 'app.py').read_text(encoding='utf-8-sig'))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'get_verse_of_the_day')
        references = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == 'VERSE_REFERENCES' for target in node.targets))
        zone = ZoneInfo('Asia/Manila')
        local_midnight = datetime(2026, 10, 21, 0, 1, tzinfo=zone)
        clock = Mock()
        clock.now.return_value = local_midnight
        fetch = Mock(return_value='Synthetic NIV daily text.')
        legacy = Mock(side_effect=AssertionError('Do not read or update the old KJV cache'))
        namespace = dict(datetime=clock, PH_TZ=zone, VERSE_REFERENCES=references, app=self.app,
                         DATABASE=str(Path(self.directory.name) / 'app_v2.db'), AI_DATABASE=str(Path(self.directory.name) / 'ai_index.db'),
                         get_indexed_niv_passage=fetch, get_db=legacy,
                         requests=Mock(get=Mock(side_effect=AssertionError('No third-party verse requests'))))
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'app.py', 'exec'), namespace)
        get_daily = namespace['get_verse_of_the_day']
        expected = references[local_midnight.date().toordinal() % len(references)]
        self.assertEqual(get_daily(), (expected, 'Synthetic NIV daily text.'))
        clock.now.assert_called_once_with(zone)
        self.assertEqual(fetch.call_args.args, (expected, str(self.path)))
        legacy.assert_not_called()
        fetch.return_value = None
        self.assertEqual(get_daily(today=local_midnight.date()), (expected, "Today's NIV verse is temporarily unavailable."))
        self.assertFalse(self.path.exists())

    def test_bulletin_template_identifies_niv_and_escapes_source_text(self):
        from flask import render_template
        with self.app.test_request_context():
            html = render_template('bulletin.html', verse_reference='Genesis 1:1', verse_text='<script>fixture</script>',
                                   today_str='October 21, 2026', area_number='1', posts=[], current_church='Fixture Church')
        self.assertIn('Genesis 1:1 (NIV)', html)
        self.assertIn('New International Version (NIV)', html)
        self.assertNotIn('bible-api.com', html)
        self.assertNotIn('KJV', html)
        self.assertIn('&lt;script&gt;fixture&lt;/script&gt;', html)

    def test_sync_is_developer_only_and_rejects_untrusted_sources_and_csrf(self):
        headers = self.preview()
        self.assertIn('id="bsSync"', self.client.get(self.base).get_data(as_text=True))
        self.assertEqual(self.client.post(self.base + '/api/sync', json={}).status_code, 400)
        self.assertEqual(self.client.post(self.base + '/api/sync', json={'url': 'https://example.test'}, headers=headers).status_code, 400)
        for username, pastor in (('OtherAO', False), ('Pastor', True)):
            self.login(username, pastor=pastor)
            page = self.client.get(self.base)
            self.assertNotIn('id="bsSync"', page.get_data(as_text=True))
            with self.client.session_transaction() as account:
                token = account['bible_study_csrf']
            self.assertEqual(self.client.get(self.base + '/api/sync').status_code, 403)
            self.assertEqual(self.client.post(self.base + '/api/sync', json={}, headers={'X-CSRF-Token': token}).status_code, 403)
        self.assertFalse(self.path.exists())

    def test_source_discovery_is_read_only_and_rejects_ambiguous_books(self):
        path = Path(self.directory.name) / 'library.db'
        with closing(sqlite3.connect(path)) as conn, conn:
            conn.execute('CREATE TABLE pastor_library_books(id INTEGER,title TEXT,manual_title TEXT,is_active INTEGER)')
            conn.execute('CREATE TABLE pastor_library_files(book_id INTEGER,name TEXT,drive_file_id TEXT,size INTEGER,format TEXT,is_active INTEGER,is_duplicate INTEGER)')
            conn.executemany('INSERT INTO pastor_library_books VALUES(?,?,NULL,1)', [(1, 'NIV fixture'), (2, 'Suite fixture')])
            conn.executemany("INSERT INTO pastor_library_files VALUES(?,?,?,100,'EPUB',1,0)",
                             [(1, 'Holy Bible (NIV).epub', 'fixture_niv_id'), (2, 'Ultimate Bible Study Suite.epub', 'fixture_suite_id')])
        before = path.read_bytes()
        sources = self.bible._library_sources(path)
        self.assertEqual(sources['NIV']['drive_file_id'], 'fixture_niv_id')
        self.assertEqual(sources['KJV']['drive_file_id'], 'fixture_suite_id')
        self.assertEqual(path.read_bytes(), before)
        with closing(sqlite3.connect(path)) as conn, conn:
            conn.execute("INSERT INTO pastor_library_files VALUES(1,'Holy Bible (NIV).epub','duplicate_id',100,'EPUB',1,0)")
        with self.assertRaises(self.bible.SyncError):
            self.bible._library_sources(path)
        with self.assertRaises(self.bible.SyncError):
            self.bible._download_source(Mock(), {'drive_file_id': '../../unsafe'})

    def test_explicit_sync_initializes_and_publishes_in_background(self):
        import time
        headers = self.preview()
        niv = self.fixture_epub([('niv.html', '<p id="xhtmlv01001001"><sup>1</sup>Alpha.</p>')])
        selected = {'NIV': dict(name='Holy Bible (NIV).epub', title='NIV fixture', drive_file_id='fixture_niv_id', fingerprint={'modifiedTime':'now'})}
        drive = Mock()
        with patch.object(self.bible, '_official_drive_sources', return_value=('fixture_folder_id', selected, [])), \
             patch.object(resources, 'get_drive_session', return_value=drive), \
             patch.object(self.bible, '_download_source', return_value=niv), \
             patch.object(self.bible, '_validate_niv_source') as validate:
            self.assertFalse(self.path.exists())
            self.assertEqual(self.client.post(self.base + '/api/sync', json={}, headers=headers).status_code, 202)
            deadline = time.monotonic() + 5
            lock = None
            while lock is None and time.monotonic() < deadline:
                time.sleep(.01)
                lock = self.bible._sync_lock(self.path)
            self.assertIsNotNone(lock)
            self.bible._release_sync_lock(lock)
            validate.assert_called_once()
            drive.close.assert_called_once()
        self.assertEqual(self.client.get(self.base + '/api/sync').json['sync']['status'], 'completed')
        self.assertEqual(self.client.get(self.base + '/api/status').json['indexed_verses'], 1)
        self.assertEqual(self.client.get(self.base + '/api/chapter').json['verses'][0]['text'], 'Alpha.')

    def test_sync_job_lock_status_and_failure_keep_previous_active_data(self):
        from threading import Event
        headers = self.preview(initialize=True)
        self.publish_fixture()
        started, release, done = Event(), Event(), Event()
        def blocked_sync(path, library_path, progress, **options):
            progress('Testing an in-progress job.')
            started.set()
            try:
                release.wait(timeout=5)
                raise self.bible.SyncError('Fixture source validation failed; existing data was kept.')
            finally:
                done.set()
        with patch.object(self.bible, 'sync_sources', side_effect=blocked_sync), patch.object(self.bible.logging, 'getLogger'):
            response = self.client.post(self.base + '/api/sync', json={}, headers=headers)
            self.assertEqual(response.status_code, 202)
            try:
                self.assertTrue(started.wait(timeout=5))
                self.assertEqual(self.client.get(self.base + '/api/sync').json['sync']['status'], 'running')
                self.assertEqual(self.client.post(self.base + '/api/sync', json={}, headers=headers).status_code, 409)
                self.assertIsNone(self.bible._sync_lock(self.path))
            finally:
                release.set()
                self.assertTrue(done.wait(timeout=5))
                # Acquire the OS lock only after the worker has finished its finally block.
                import time
                deadline = time.monotonic() + 5
                lock = None
                while lock is None and time.monotonic() < deadline:
                    time.sleep(.01)
                    lock = self.bible._sync_lock(self.path)
                self.assertIsNotNone(lock)
                self.bible._release_sync_lock(lock)
        self.assertEqual(self.client.get(self.base + '/api/sync').json['sync']['status'], 'failed')
        self.assertEqual(self.client.get(self.base + '/api/status').json['indexed_verses'], 1)

    def test_harper_images_tables_emphasis_and_reverse_glosses_are_served_from_study_only(self):
        self.preview(initialize=True)
        from PIL import Image
        image=io.BytesIO();Image.new('RGB',(3,3),'black').save(image,format='PNG')
        epub=self.fixture_epub([('ch-a.xhtml','<p><a id="a1" href="toc-a.xhtml#a_1"><span class="bold">Alpha</span></a> explanation.</p>'
            '<p>Source <span class="italic">emphasis</span>.</p><p><img src="map.png"/></p>'
            '<table><tr><th>Heading</th><td>Value</td></tr></table>'
            '<p><a id="a2" href="toc-a.xhtml#a_2">Another</a> entry.</p>')],
            {'map.png':image.getvalue(),'style.css':b'.bold{font-weight:bold}.italic{font-style:italic}'})
        harper=self.bible.parse_harper_epub(epub)
        self.assertEqual(len(harper['entries']),2)
        self.assertEqual(harper['entries'][0]['blocks'][-1]['type'],'table')
        self.assertIn('strong',harper['entries'][0]['blocks'][0]['runs'][0]['marks'])
        self.assertIn('em',next(r for r in harper['entries'][0]['blocks'][1]['runs'] if r['text']=='emphasis')['marks'])
        parsed={'harper':harper}
        for family,code,word in [('tagalog','tl','pag-ibig'),('cebuano','ceb','gugma')]:
            data=json.dumps(dict(word=word,lang_code=code,pos='noun',senses=[dict(glosses=['love; affection'])])).encode()
            parsed[family]=self.bible.parse_language_dictionary(data,family)
        selected={family:dict(title=family+' fixture',drive_file_id='fixture_'+family+'_id',sha256=str(i)*64,fingerprint={}) for i,family in enumerate(parsed,1)}
        self.bible._publish_dictionary_sync(self.path,selected,parsed,'fixture_folder_id',[])
        results=self.client.get(self.base+'/api/dictionaries/search?q=love').json
        self.assertEqual([g['label'] for g in results['translations']],['Tagalog','Cebuano'])
        self.assertEqual([g['entries'][0]['headword'] for g in results['translations']],['pag-ibig','gugma'])
        self.assertEqual([g['label'] for g in results['sources']],['HarperCollins','Vine','Mounce'])
        entry=self.client.get(self.base+'/api/dictionaries/search?q=alpha').json['sources'][0]['entries'][0]
        full=self.client.get(self.base+'/api/dictionaries/entry/'+str(entry['id'])).json['entry']
        digest=next(r['asset'] for block in full['blocks'] for r in block['runs'] if r['type']=='image')
        self.assertEqual(self.client.get(self.base+'/api/dictionaries/asset/'+digest).data,image.getvalue())
        with self.assertRaises(self.bible.SyncError):self.bible.parse_language_dictionary(b'{broken','tagalog')

    def test_native_word_study_uses_own_dictionary_forms_and_explicit_english_meanings(self):
        self.preview(initialize=True);self.publish_fixture()
        records={
            'tagalog':[
                dict(word='pag-ibig',pos='noun',senses=[dict(glosses=['love; affection'],examples=[dict(text='Example',english='sparkling')])]),
                dict(word='mahalin',pos='verb',forms=[dict(form='minahal')],senses=[dict(glosses=['to love'])]),
                dict(word='maganda',pos='verb',senses=[dict(glosses=['to be beautiful'])]),
                dict(word='tubo',pos='noun',senses=[dict(glosses=['profit'])])],
            'cebuano':[
                dict(word='gugma',pos='noun',senses=[dict(glosses=['love'])]),
                dict(word='tubo',pos='verb',senses=[dict(glosses=['to grow'])])]}
        parsed={}
        for family,items in records.items():
            code='tl' if family=='tagalog' else 'ceb'
            parsed[family]=self.bible.parse_language_dictionary(b'\n'.join(json.dumps(dict(r,lang_code=code)).encode() for r in items),family)
        mounce=self.fixture_epub([('main.html','<p class="h2" id="ch5">BEAUTIFUL</p><p class="h1">New Testament</p>'
            '<p><b>Adjective:</b> kalos (<i>kalos</i>), GK 2819 (S 2566), 101x. Beautiful.</p>')])
        parsed['mounce']=self.bible.parse_dictionary_epub(mounce,'mounce')
        selected={f:dict(title=f,drive_file_id='fixture_'+f,sha256=str(i)*64,fingerprint={}) for i,f in enumerate(parsed,1)}
        self.bible._publish_dictionary_sync(self.path,selected,parsed,'fixture_folder_id',[])
        self.bible.import_usfm_translations(self.path,{'TCB':self.translation_zip('pag-ibig'),'CEBOCB':self.translation_zip('gugma')})
        with closing(sqlite3.connect(self.path)) as conn:
            before=conn.execute('SELECT id,text,content_hash FROM bible_verses ORDER BY id').fetchall()
        for translation,word in [('TCB','pag-ibig'),('CEBOCB','gugma')]:
            verse=self.client.get(self.base+'/api/chapter',query_string=dict(book='Genesis',chapter=1,translation=translation)).json['verses'][0]
            with (patch.object(self.bible,'verified_word_study',side_effect=AssertionError('No native-to-NIV alignment')),
                  patch.object(self.bible,'search_dictionaries',side_effect=AssertionError('English lookup requires the reader choice'))):
                data=self.client.get(f"{self.base}/api/word/{verse['id']}/0").json
            self.assertEqual(data['result_type'],'native_dictionary');self.assertEqual(data['query'],word)
            self.assertFalse(data['verified']);self.assertEqual(data['sources'],[]);self.assertNotIn('original_tokens',data)
            self.assertIn('may not match',data['accuracy_notice']);self.assertEqual(data['dictionary']['source'],self.bible.NATIVE_DICTIONARY_FAMILIES[translation])
            entry=data['dictionary']['entries'][0];self.assertEqual(entry['headword'],word)
            meanings=[choice['word'] for choice in entry['english_equivalents']]
            self.assertIn('love',meanings);self.assertNotIn('sparkling',meanings)
        def native(word,translation):return self.client.get(self.base+'/api/dictionaries/search',query_string=dict(q=word,translation=translation)).json
        recorded=native('minahal','TCB')['dictionary']['entries'][0]
        self.assertEqual(recorded['headword'],'mahalin');self.assertTrue(recorded['is_recorded_form'])
        stative=native('maganda','TCB')['dictionary']['entries'][0]
        self.assertEqual(stative['part'],'verb');self.assertEqual(stative['english_equivalents'][0]['part'],'adjective')
        self.assertEqual(stative['english_equivalents'][0]['word'],'beautiful')
        self.assertEqual(native('tubo','TCB')['dictionary']['entries'][0]['english_equivalents'][0]['word'],'profit')
        self.assertEqual(native('tubo','CEBOCB')['dictionary']['entries'][0]['english_equivalents'][0]['word'],'grow')
        missing=native('gihigugma','CEBOCB')
        self.assertEqual(missing['dictionary']['entries'],[]);self.assertIn('gugma',[s['word'] for s in missing['suggestions']])
        self.assertEqual(missing['suggestions'][0]['preview'],'love')
        self.assertFalse(native('loved','TCB')['dictionary']['entries'])
        english=self.client.get(self.base+'/api/dictionaries/search?q=beautiful&part=adjective').json
        self.assertEqual(english['sources'][2]['entries'][0]['headword'],'BEAUTIFUL')
        self.assertEqual(self.client.get(self.base+'/api/dictionaries/search?q=love&translation=bad').status_code,400)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT id,text,content_hash FROM bible_verses ORDER BY id').fetchall(),before)

    def test_dictionary_cross_references_and_shortened_citations_keep_source_typography(self):
        self.preview(initialize=True)
        epub=self.fixture_epub([('ch-s.xhtml',
            '<p><a id="s1" href="toc-s.xhtml#s_1"><b>sapphire</b></a>, source text. Exod. 24:10; Ezek. 1:26; 10:1 may refer to this stone. '
            '<b><i>See also</i></b> breastpiece; jewels, jewelry; unavailable suggestion.</p>'
            '<p>Gen. 24 records an event (24:58). A statistic (365) and a different chapter (9:10) stay plain.</p>'
            '<p>A limited suggestion (see unavailable) mentions jewels later in prose.</p>'
            '<p><a id="s2" href="toc-s.xhtml#s_2">breastpiece</a>, an article. See sapphire.</p>'
            '<p><a id="s3" href="toc-s.xhtml#s_3">jewels, jewelry</a>, an article.</p>')])
        parsed=self.bible.parse_harper_epub(epub)
        source=dict(title='Harper fixture',drive_file_id='harper_fixture',sha256='c'*64,fingerprint={})
        self.bible._publish_dictionary_sync(self.path,{'harper':source},{'harper':parsed},'fixture_folder_id',[])
        with closing(sqlite3.connect(self.path)) as conn:
            entry,stored=conn.execute("SELECT id,content_json FROM bible_dictionary_entries WHERE headword='sapphire'").fetchone()
        data=self.client.get(f'{self.base}/api/dictionaries/entry/{entry}').json['entry'];blocks=data['blocks']
        self.assertEqual(blocks[0]['text'],parsed['entries'][0]['blocks'][0]['text'])
        self.assertEqual(blocks[0]['runs'],parsed['entries'][0]['blocks'][0]['runs'])
        links=blocks[0]['dictionary_links']
        self.assertEqual([blocks[0]['text'][l['start']:l['end']] for l in links],['breastpiece','jewels, jewelry'])
        for link in links:
            target=self.client.get(f"{self.base}/api/dictionaries/entry/{link['entry_ids'][0]}").json['entry']
            self.assertEqual(target['headword'],link['headword'])
        self.assertEqual([(r['book'],r['chapter'],r['verse']) for r in blocks[0]['references']],
                         [('Exodus',24,10),('Ezekiel',1,26),('Ezekiel',10,1)])
        chapter,bare=blocks[1]['references']
        self.assertTrue(chapter['chapter_only']);self.assertEqual((bare['book'],bare['verse']),('Genesis',58))
        self.assertFalse(blocks[2].get('dictionary_links'))
        preview=self.client.get(self.base+'/api/dictionaries/search?q=sapphire').json['sources'][0]['entries'][0]['preview_block']
        self.assertTrue(preview['dictionary_links']);self.assertIn('strong',preview['runs'][0]['marks'])
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT content_json FROM bible_dictionary_entries WHERE id=?',(entry,)).fetchone()[0],stored)

    def test_compound_word_clicks_and_article_mentions_find_embedded_material(self):
        self.preview(initialize=True);self.publish_fixture()
        parsed={family:self.bible.parse_dictionary_epub(data,family) for family,data in self.dictionary_fixtures().items()}
        for family in ('mounce','vine'):
            entry=parsed[family]['entries'][0]
            block=dict(type='paragraph',text='An embedded explanation of lapis lazuli and turquoise.',references=[],
                       runs=[dict(type='text',text='An embedded explanation of lapis lazuli and turquoise.',marks=[])])
            entry['blocks'].append(block);entry['plain_text']+='\n\n'+block['text']
        harper=self.fixture_epub([('ch-s.xhtml','<p><a id="s1" href="toc-s.xhtml#s_1">sapphire</a>, an original source article.</p>'
            '<p>'+('Introductory background. '*30)+'The references may refer to lapis lazuli. Original closing context.</p>'
            '<p><a id="s2" href="toc-s.xhtml#s_2">stone</a>, including turquoise.</p>'
            '<p><a id="s3" href="toc-s.xhtml#s_3">unrelated</a>, a turquoiselike color is not a whole-word match.</p>')])
        parsed['harper']=self.bible.parse_harper_epub(harper)
        selected={family:dict(title=family+' fixture',drive_file_id='fixture_'+family,sha256=str(i)*64,fingerprint={}) for i,family in enumerate(parsed,1)}
        self.bible._publish_dictionary_sync(self.path,selected,parsed,'fixture_folder_id',[])
        text='Stones of turquoise, your foundations with lapis lazuli.'
        with closing(sqlite3.connect(self.path)) as conn,conn:
            edition=conn.execute("SELECT id FROM bible_editions WHERE translation='NIV' AND is_active=1").fetchone()[0]
            verse=conn.execute('''INSERT INTO bible_verses(edition_id,book_id,chapter,verse,text,content_hash)
                VALUES(?,23,54,11,?,?)''',(edition,text,hashlib.sha256(text.encode()).hexdigest())).lastrowid
            for index,match in enumerate(re.finditer(r'\w+',text)):
                conn.execute('INSERT INTO bible_tokens(verse_id,token_index,start_offset,end_offset,text) VALUES(?,?,?,?,?)',
                    (verse,index,match.start(),match.end(),match[0]))
            conn.execute("INSERT INTO bible_edition_details(edition_id,label,source_kind) VALUES(?,'Fixture','thompson_pdf')",(edition,))
            for key,title,blocks in [
                ('2858','PRECIOUS STONES',[dict(type='heading',text='o. Lapis Lazuli/Sapphire,'),dict(type='paragraph',text='Isa 54:11',references=self.bible.scripture_references('Isa 54:11'))]),
                ('2850','PRECIOUS STONES',[dict(type='heading',text='g. Turquoise,'),dict(type='paragraph',text='Rev 21:20',references=self.bible.scripture_references('Rev 21:20'))]),
                ('777','COMFORT',[dict(type='paragraph',text='A quoted verse with lapis lazuli and turquoise.')])]:
                conn.execute('INSERT INTO bible_chain_topics(edition_id,topic_key,title,content_json,source_page) VALUES(?,?,?,?,99)',
                    (edition,key,title,json.dumps(blocks)))
            indices=dict(conn.execute('SELECT lower(text),token_index FROM bible_tokens WHERE verse_id=?',(verse,)))
        for word in ('lapis','lazuli'):
            data=self.client.get(f'{self.base}/api/word/{verse}/{indices[word]}').json
            self.assertEqual(data['query'],'lapis lazuli');self.assertFalse(data['verified'])
            self.assertEqual(data['lookup_token_indices'],[indices['lapis'],indices['lazuli']])
            for source in data['sources']:
                self.assertFalse(source['entries'])
                self.assertTrue(source['related_entries'])
                self.assertEqual(source['related_entries'][0]['match_type'],'article_text')
                self.assertIn('lapis lazuli',source['related_entries'][0]['preview'])
            self.assertEqual(data['sources'][0]['related_entries'][0]['headword'],'sapphire')
        result=self.client.get(self.base+'/api/dictionaries/search?q=turquoise').json
        self.assertEqual([e['headword'] for e in result['sources'][0]['related_entries']],['stone'])
        for query,heading,key in [('lapis lazuli','Lapis Lazuli/Sapphire','2858'),('lazuli','Lapis Lazuli/Sapphire','2858'),('turquoise','Turquoise','2850')]:
            result=self.client.get(self.base+'/api/concordance',query_string={'q':query}).json
            self.assertTrue(result['available']);self.assertEqual(len(result['entries']),1)
            self.assertEqual(result['entries'][0]['headword'],heading)
            self.assertEqual(result['entries'][0]['topic_key'],key)
            self.assertTrue(result['entries'][0]['blocks'][-1]['references'])
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT text FROM bible_verses WHERE id=?',(verse,)).fetchone()[0],text)

    def test_word_types_guide_equivalents_while_dictionary_articles_stay_complete(self):
        self.preview(initialize=True);self.publish_fixture()
        mounce=self.fixture_epub([('main.html',
            '<p class="h2" id="ch5">COMFORT</p><p class="h1">Old Testament</p>'
            '<p><b>Verb:</b> naham (<i>naham</i>), GK 5714 (S 5162), 108x. To comfort.</p>'
            '<p class="h1">New Testament</p><p><b>Verb:</b> parakaleo (<i>parakaleo</i>), GK 4151 (S 3870), 109x. To comfort.</p>'
            '<p><b>Noun:</b> paraklesis (<i>paraklesis</i>), GK 4155 (S 3874), 29x. Consolation.</p>'
            '<p class="h2" id="ch5b">CHANGE</p><p class="h1">New Testament</p><p><b>Verb:</b> strepho (<i>strepho</i>), GK 5138 (S 4762), 21x. See <i>comfort</i>.</p>'
            '<p class="h2" id="ch5c">LOVE</p><p class="h1">New Testament</p><p><b>Verb:</b> agapao (<i>agapao</i>), GK 26 (S 25), 143x. To love.</p>'
            '<p><b>Noun:</b> agape (<i>agape</i>), GK 27 (S 26), 116x. Love.</p>'
            '<p class="h2" id="ch5d">KING</p><p class="h1">New Testament</p>'
            '<p><b>Noun:</b> basileus (<i>basileus</i>), GK 995 (S 935), 115x. King.</p>'
            '<p><b>Verb:</b> basileuo (<i>basileuo</i>), GK 996 (S 936), 21x. Reign.</p>'
            '<p><b>Adjective:</b> basilikos (<i>basilikos</i>), GK 997 (S 937), 5x. Royal.</p>')])
        vine=self.fixture_epub([('vine-ntA.html','<div class="chap" id="G.COMFORT"><p class="bh">COMFORT</p>'
            '<p class="pc">A. Nouns.</p><p><i>paraklesis</i> (3874), consolation.</p>'
            '<p class="pc">B. Verbs.</p><p><i>parakaleo</i> (3870), to comfort.</p></div>')])
        harper=self.fixture_epub([('ch-j.xhtml',
            '<p><a id="j1" href="toc-j.xhtml#j_1">jewels, jewelry</a>, precious objects.</p>'
            '<p><a id="j2" href="toc-j.xhtml#j_2">theater</a>, cushions made the audience comfortable; people were comforted.</p>'
            '<p><a id="j3" href="toc-j.xhtml#j_3">twelve, the</a>, the apostles.</p>')])
        parsed={f:self.bible.parse_dictionary_epub(data,f) for f,data in [('mounce',mounce),('vine',vine)]}
        parsed['harper']=self.bible.parse_harper_epub(harper)
        for family,code in [('tagalog','tl'),('cebuano','ceb')]:
            records=[dict(word=word,pos=pos,lang_code=code,senses=[dict(glosses=[gloss])]) for word,pos,gloss in
                     [('noun-choice','noun','comfort'),('verb-choice','verb','to comfort'),('love-noun','noun','love'),('love-verb','verb','to love')]]
            parsed[family]=self.bible.parse_language_dictionary(b'\n'.join(json.dumps(r).encode() for r in records),family)
        selected={f:dict(title=f,drive_file_id='fixture_'+f,sha256=str(i)*64,fingerprint={}) for i,f in enumerate(parsed,1)}
        self.bible._publish_dictionary_sync(self.path,selected,parsed,'fixture_folder_id',[])
        verses={}
        with closing(sqlite3.connect(self.path)) as conn,conn:
            edition=conn.execute("SELECT id FROM bible_editions WHERE translation='NIV' AND is_active=1").fetchone()[0]
            for number,text in enumerate(['Afflicted city, not comforted, I will rebuild you.',
                    'Your gates of sparkling jewels.', 'God is love.', 'Love one another.', 'He has comforted them.',
                    'God so loved the world.', 'The king of the city.'],11):
                verse=conn.execute('INSERT INTO bible_verses(edition_id,book_id,chapter,verse,text,content_hash) VALUES(?,23,54,?,?,?)',
                    (edition,number,text,hashlib.sha256(text.encode()).hexdigest())).lastrowid
                tokens=list(re.finditer(r'\w+',text));verses[number]=(verse,tokens)
                for i,m in enumerate(tokens):conn.execute('INSERT INTO bible_tokens(verse_id,token_index,start_offset,end_offset,text) VALUES(?,?,?,?,?)',(verse,i,m.start(),m.end(),m[0]))
        def lookup(number,word,part=None):
            verse,tokens=verses[number];index=next(i for i,m in enumerate(tokens) if m[0].lower()==word)
            return self.client.get(f'{self.base}/api/word/{verse}/{index}',query_string={'part':part} if part else {}).json
        for number in (11,15):
            data=lookup(number,'comforted');self.assertEqual(data['word_type']['part'],'verb')
            self.assertFalse(data['sources'][0]['entries']);self.assertEqual(data['sources'][0]['related_entries'][0]['headword'],'theater')
            for source in data['sources'][1:]:
                self.assertTrue(source['entries']);self.assertFalse(source['other_entries'])
                for entry in source['entries']:
                    self.assertEqual(entry['headword'],'COMFORT')
                    self.assertTrue(entry['full_article']);self.assertNotIn('definitions',entry)
                    full=self.client.get(f"{self.base}/api/dictionaries/entry/{entry['id']}").json['entry']
                    self.assertTrue(any('Noun' in b['text'] for b in full['blocks']))
                    self.assertTrue(any('Verb' in b['text'] for b in full['blocks']))
            for translation in data['translations']:self.assertEqual([e['headword'] for e in translation['entries']],['verb-choice'])
            self.assertFalse(data['verified'])
        self.assertEqual(lookup(12,'sparkling')['word_type']['part'],'adjective')
        self.assertEqual(lookup(13,'love')['word_type']['part'],'noun')
        self.assertEqual(lookup(14,'love')['word_type']['part'],'verb')
        self.assertEqual(lookup(16,'loved')['word_type']['part'],'verb')
        self.assertEqual(lookup(17,'king')['word_type']['part'],'noun')
        data=lookup(11,'comforted','noun')
        self.assertEqual(data['word_type'],dict(part='noun',basis='chosen'))
        def articles(groups):
            return [(g['source'],[(e['id'],e['headword'],e['preview']) for e in g['entries']]) for g in groups]
        self.assertEqual(articles(data['sources'][1:]),articles(lookup(11,'comforted','verb')['sources'][1:]))
        self.assertEqual([e['headword'] for e in data['translations'][0]['entries']],['noun-choice'])
        self.assertEqual(self.client.get(self.base+'/api/dictionaries/search?q=jewels').json['sources'][0]['entries'][0]['headword'],'jewels, jewelry')
        grammar=self.client.get(self.base+'/api/dictionaries/search?q=the').json
        self.assertEqual(grammar['word_type']['part'],'grammar')
        self.assertTrue(all(not s['related_entries'] for s in grammar['sources']))
        self.assertTrue(all(not s['entries'] and not s['other_entries'] for s in grammar['sources']))
        self.assertEqual(self.client.get(self.base+'/api/dictionaries/search?q=love&part=invalid').status_code,400)
        verse,_=verses[11]
        self.assertEqual(self.client.get(f'{self.base}/api/word/{verse}/0?part=invalid').status_code,400)

    def test_reverse_love_prioritizes_bible_vocabulary_and_rejects_incidental_examples(self):
        self.preview(initialize=True)
        records={
            'tagalog':[
                ('ibig','noun',['want; desire','love; affection']),('amor','noun',['love; affair']),
                ('pag-ibig','noun',['love; attraction; fondness']),('pagmamahal','noun',['love; act of loving']),
                ('kuyom','noun',["withholding (of one's anger, love, etc.)"]),
                ('talang','noun',['blush (from embarrassment, love, etc.)'])],
            'cebuano':[
                ('gugma','noun',['love']),('pangga','verb',['to love']),
                ('pinangga','adj',['loved; beloved']),('pinangga','noun',['a person who is dear or loved']),
                ('gihalaran','noun',["the object of one's offering, sacrifice, love or affections"]),
                ('Venus','name',['the goddess of love'])]}
        parsed={};selected={}
        for index,(family,items) in enumerate(records.items(),1):
            code='tl' if family=='tagalog' else 'ceb'
            data=b'\n'.join(json.dumps(dict(word=word,pos=pos,lang_code=code,senses=[dict(glosses=glosses)])).encode() for word,pos,glosses in items)
            parsed[family]=self.bible.parse_language_dictionary(data,family)
            selected[family]=dict(title=family,drive_file_id='fixture_'+family,sha256=str(index)*64,fingerprint={})
        self.bible._publish_dictionary_sync(self.path,selected,parsed,'fixture_folder_id',[])
        with closing(sqlite3.connect(self.path)) as conn,conn:
            # Old indexes incorrectly gave parenthetical/comma fragments a high
            # weight. Search must reject these without requiring another import.
            conn.execute('''INSERT OR REPLACE INTO bible_dictionary_gloss_terms
                SELECT id,'love',4 FROM bible_dictionary_entries WHERE headword IN ('kuyom','talang','gihalaran','Venus')''')
            source,run=conn.execute('SELECT source_id,import_run_id FROM bible_dictionary_versions LIMIT 1').fetchone()
            edition=conn.execute('''INSERT INTO bible_editions(source_id,import_run_id,edition_key,translation,title,status,is_active)
                VALUES(?,?,'fixture_TCB','TCB','Fixture','ready',1)''',(source,run)).lastrowid
            verse=conn.execute('''INSERT INTO bible_verses(edition_id,book_id,chapter,verse,text,content_hash)
                VALUES(?,1,1,1,'pag-ibig pag-ibig pagmamahal ibig',?)''',(edition,'f'*64)).lastrowid
            offset=0
            for index,word in enumerate(['pag-ibig','pag-ibig','pagmamahal','ibig']):
                conn.execute('''INSERT INTO bible_tokens(verse_id,token_index,start_offset,end_offset,text,normalized_text)
                    VALUES(?,?,?,?,?,?)''',(verse,index,offset,offset+len(word),word,word));offset+=len(word)+1
        for query in ('love','loved'):
            data=self.client.get(self.base+'/api/dictionaries/search',query_string={'q':query}).json
            tagalog,cebuano=data['translations']
            words=[e['headword'] for e in tagalog['entries']]
            self.assertEqual(words[:2],['pag-ibig','pagmamahal'])
            self.assertIn('ibig',words);self.assertNotIn('kuyom',words);self.assertNotIn('talang',words)
            words=[e['headword'] for e in cebuano['entries']]
            self.assertIn('gugma',words);self.assertIn('pangga',words)
            self.assertNotIn('gihalaran',words);self.assertNotIn('Venus',words)
            if query=='loved':
                entry=next(e for e in cebuano['entries'] if e['headword']=='pinangga')
                self.assertEqual(len(entry['entry_ids']),2)
                for entry_id in entry['entry_ids']:
                    self.assertEqual(self.client.get(self.base+'/api/dictionaries/entry/'+str(entry_id)).status_code,200)

    def test_be_psalm_articles_do_not_pull_headings_from_incidental_mentions(self):
        self.preview(initialize=True)
        text23='The shepherd cares for his sheep. Psalm 2 and Psalm 22 are discussed elsewhere in the source.'
        text22='This is the complete neighboring Psalm 22 article, which must not appear in the Psalm 23 reader.'
        text2='This is the complete neighboring Psalm 2 article, which must remain separate from the other psalms.'
        heading='Care for the flock'
        epub=self.fixture_epub([('chapter.xhtml',
            '<p>Chapter One</p><p>Source chapter title</p><p>(Psalms 1–41)</p>'
            '<h2>Psalm 2</h2><p>'+text2+'</p>'
            '<h2>Psalm 22</h2><p>'+text22+'</p>'
            '<h2>Psalm 23</h2><p>'+text23+'</p><p><strong>'+heading+'</strong></p>'
            '<p>The final source paragraph must keep its <em>original emphasis</em> and complete wording.</p>')],
            metadata='<title>Be Worshipful: Psalms 1–89</title><creator>Warren W. Wiersbe</creator>')
        content=self.bible.parse_be_epub(epub,'Be Worshipful (Psalms 1-89)')
        section=next(section for section in content['sections'] if section['passage']=='Psalms 23')
        self.assertEqual([block['text'] for block in section['blocks']][:2],[text23,heading])
        self.assertFalse(any(block['text'] in ('Psalm 2','Psalm 22','Psalm 23') for block in section['blocks']))
        self.assertEqual(section['blocks'][1]['type'],'heading')
        self.assertTrue(any('em' in run.get('marks',[]) or 'italic' in run.get('marks',[])
                            for run in section['blocks'][-1]['runs']))
        selected={'be:psalms_fixture':dict(title=content['title'],drive_file_id='psalms_fixture',
            sha256='f'*64,fingerprint={})}
        self.bible._publish_dictionary_sync(self.path,selected,{'be:psalms_fixture':content},'fixture_folder_id',[])
        # Recreate the old import bug: short titles were selected because they
        # occurred somewhere inside the article's prose, twice in this EPUB.
        with closing(sqlite3.connect(self.path)) as conn,conn:
            row=conn.execute("SELECT id,content_json FROM bible_commentary_sections WHERE passage='Psalms 23'").fetchone()
            old_blocks=[dict(type='heading',text=word,runs=[]) for word in ('Psalm 2','Psalm 22','Psalm 23')]
            conn.execute('UPDATE bible_commentary_sections SET content_json=? WHERE id=?',
                (json.dumps(old_blocks+json.loads(row[1])+old_blocks),row[0]))
        before=self.path.read_bytes()
        result=self.client.get(self.base+'/api/commentary?book=Psalms&chapter=23&verse=1').json
        self.assertTrue(result['available'])
        self.assertEqual(len(result['entries']),1)
        self.assertEqual([block['text'] for block in result['entries'][0]['blocks']],
                         [block['text'] for block in section['blocks']])
        self.assertEqual(self.path.read_bytes(),before)

    def test_be_import_and_cleanup_keep_pdf_and_unimported_editions(self):
        self.preview(initialize=True)
        epub=self.fixture_epub([('chapter.xhtml','<p>Chapter One</p><p>Source heading</p><p>(2 Peter 1:1–11)</p>'
            '<p>A complete synthetic source paragraph with <span class="italic">original emphasis</span> and enough text for validation.</p>'
            '<blockquote><p>A preserved quotation from the source.</p></blockquote>')],
            {'style.css':b'.italic{font-style:italic}'},'<title>Be Alert: An Expository Study of 2 Peter</title><creator>Warren W. Wiersbe</creator>')
        content=self.bible.parse_be_epub(epub,'Be Alert (2 Peter)')
        selected={'be:fixture_be_id':dict(title=content['title'],drive_file_id='fixture_be_id',sha256='e'*64,fingerprint={})}
        ai=Path(self.directory.name)/'ai.db'
        with closing(sqlite3.connect(ai)) as conn,conn:
            conn.execute('CREATE TABLE pij_library_documents(id INTEGER,drive_file_id TEXT,title TEXT,author TEXT,format TEXT,source_type TEXT)')
            conn.execute('CREATE TABLE pij_library_chunks(document_id INTEGER,content TEXT)')
            conn.executemany('INSERT INTO pij_library_documents VALUES(?,?,?,?,?,?)',[
                (1,'old_be_id','Be Alert (2 Peter)','Warren W. Wiersbe','epub','public_ebook'),
                (2,'pdf_be_id','Be Alert (2 Peter)','Warren W. Wiersbe','pdf','public_ebook'),
                (3,'not_imported','Other study source','Author','epub','public_ebook')])
            conn.executemany('INSERT INTO pij_library_chunks VALUES(?,?)',[(1,'old'),(2,'PDF'),(3,'keep')])
        self.assertEqual(self.bible.purge_migrated_ai_sources(self.path,ai),0)
        self.bible._publish_dictionary_sync(self.path,selected,{'be:fixture_be_id':content},'fixture_folder_id',[])
        self.app.config['AI_DATABASE']=str(ai)
        data=self.client.get(self.base+'/api/commentary?book=2%20Peter&chapter=1&verse=2').json
        self.assertTrue(data['available']);self.assertEqual(len(data['entries']),1)
        self.assertTrue(any(b.get('source_quote') for b in data['entries'][0]['blocks']))
        self.assertEqual(self.client.get(self.base+'/api/commentary?book=John&chapter=1&verse=2').json['entries'],[])
        self.assertEqual(self.bible.purge_migrated_ai_sources(self.path,ai),1)
        self.assertEqual(self.bible.purge_migrated_ai_sources(self.path,ai),0)
        with closing(sqlite3.connect(ai)) as conn:
            self.assertEqual(conn.execute('SELECT id FROM pij_library_documents ORDER BY id').fetchall(),[(2,),(3,)])
            self.assertEqual(conn.execute('SELECT document_id FROM pij_library_chunks ORDER BY document_id').fetchall(),[(2,),(3,)])
        # Simulate a v7 source constraint upgrade without changing existing IDs.
        with closing(sqlite3.connect(self.path)) as conn,conn:
            conn.execute('PRAGMA user_version=7')
            before=conn.execute('SELECT * FROM bible_commentary_sections').fetchall()
        self.bible.initialize_database(self.path)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT * FROM bible_commentary_sections').fetchall(),before)
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_dictionary_import_preserves_sections_images_identifiers_and_split_entries(self):
        self.preview(initialize=True)
        self.publish_fixture()
        selected, parsed, report = self.publish_dictionaries()
        self.assertEqual(len(parsed['mounce']['entries']), 3)
        self.assertEqual(len(parsed['vine']['entries']), 3)
        self.assertNotIn('marketing', parsed['mounce']['entries'][-1]['plain_text'])
        self.assertIn('Continued original entry.', parsed['vine']['entries'][1]['plain_text'])
        self.assertFalse(parsed['vine']['entries'][2]['identifiers'])
        self.assertEqual(report['counts']['mounce_entries'], 3)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_concordance').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_lexicon').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_dictionary_assets').fetchone()[0], 1)
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])
            before = conn.execute('SELECT COUNT(*) FROM bible_dictionary_entries').fetchone()[0]
        self.bible._publish_dictionary_sync(self.path, selected, parsed, 'fixture_folder_id', [])
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_dictionary_entries').fetchone()[0], before)
        for family in parsed:
            with self.assertRaises(self.bible.SyncError):
                self.bible._validate_dictionary(parsed[family], family)

    def test_expository_epub_formatting_refresh_preserves_entries_senses_and_saved_reading(self):
        headers=self.preview(initialize=True);self.publish_fixture()
        from PIL import Image
        image=io.BytesIO();Image.new('RGB',(30,12),'black').save(image,format='PNG')
        mounce=self.fixture_epub([('main.html','<p class="h2" id="ch5">LOVE</p><p class="h1">Old Testament</p>'
            '<p class="tx"><b>Verb:</b> <img src="glyph.png"/> (<i>ahab</i>), GK 170 (S 157), 217x. To love.</p>'
            '<p class="tx1">(1) A source explanation. Gen. 1:1.</p>'
            '<p class="h1">New Testament</p><p class="tx"><b>Noun:</b> agape (<i>agape</i>), GK 27 (S 26), 116x. Love.</p>')],
            {'glyph.png':image.getvalue(),'style.css':b'.tx{text-indent:5%}.tx1{text-indent:10%}.h1{font-style:italic}'})
        vine=self.fixture_epub([('vine-ntL.html','<div id="G.LOVE"><p class="bh">LOVE (Noun and Verb)</p>'
            '<p class="pc"><b>A. Verbs.</b></p><p>1. <i>agapao</i> (25), source definition. See <a href="vine-ntL.html#G.EDIFICATION">Edify</a>.</p>'
            '<p class="pc"><b>B. Nouns.</b></p><p>1. <i>agape</i> (26), source noun definition.</p></div>'
            '<div id="G.EDIFICATION"><p class="bh">EDIFICATION</p><p>Complete referenced source article.</p></div>')],
            {'style.css':b'.pc{text-align:center}.bh{text-align:center;font-style:italic}'})
        parsed={f:self.bible.parse_dictionary_epub(data,f) for f,data in [('mounce',mounce),('vine',vine)]}
        mb=parsed['mounce']['entries'][0]['blocks']
        self.assertEqual(mb[1]['presentation']['indent_percent'],5);self.assertEqual(mb[2]['presentation']['indent_percent'],10)
        self.assertEqual(next(r for r in mb[1]['runs'] if r['type']=='image')['height_em'],.75)
        self.assertFalse(mb[1].get('illustration'));self.assertEqual(mb[0]['dictionary_style'],'testament')
        self.assertEqual(parsed['vine']['entries'][0]['blocks'][0]['presentation']['align'],'center')
        selected={f:dict(title=f,drive_file_id='fixture_'+f,sha256=str(i)*64,fingerprint={}) for i,f in enumerate(parsed,1)}
        self.bible._publish_dictionary_sync(self.path,selected,parsed,'fixture_folder_id',[])
        verse=self.client.get(self.base+'/api/chapter').json['verses'][0]
        saved=self.client.post(self.base+'/api/bookmarks',json=dict(verse_id=verse['id']),headers=headers).json['bookmark']
        with closing(sqlite3.connect(self.path)) as conn,conn:
            for row in conn.execute('SELECT id,content_json FROM bible_dictionary_entries').fetchall():
                blocks=json.loads(row[1])
                for block in blocks:
                    block.pop('presentation',None);block.pop('dictionary_style',None)
                    for run in block['runs']:run.pop('height_em',None);run.pop('epub_target',None)
                conn.execute('UPDATE bible_dictionary_entries SET content_json=? WHERE id=?',(json.dumps(blocks,ensure_ascii=False),row[0]))
            conn.execute("DELETE FROM bible_metadata WHERE key LIKE 'dictionary_presentation:%'")
            entries=conn.execute('SELECT id,version_id,locator,headword,plain_text,preview FROM bible_dictionary_entries ORDER BY id').fetchall()
            senses=conn.execute('SELECT * FROM bible_dictionary_senses ORDER BY id').fetchall()
            tokens=conn.execute('SELECT * FROM bible_tokens ORDER BY id').fetchall()
        report=self.bible._safe_publish_sources(self.path,selected,parsed,'fixture_folder_id',[],lambda message:None)
        self.assertEqual(report['formatting_entries'],3);self.assertTrue(Path(report['backup']).is_file())
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT id,version_id,locator,headword,plain_text,preview FROM bible_dictionary_entries ORDER BY id').fetchall(),entries)
            self.assertEqual(conn.execute('SELECT * FROM bible_dictionary_senses ORDER BY id').fetchall(),senses)
            self.assertEqual(conn.execute('SELECT * FROM bible_tokens ORDER BY id').fetchall(),tokens)
            before=conn.execute('SELECT id,content_json FROM bible_dictionary_entries ORDER BY id').fetchall()
        self.assertEqual(self.client.get(saved['delete_url']).json['item']['verse_id'],verse['id'])
        result=self.client.get(self.base+'/api/dictionaries/search?q=love&part=verb').json
        for group in result['sources'][1:]:
            entry=group['entries'][0]
            self.assertNotIn('definitions',entry)
            complete=self.client.get(f"{self.base}/api/dictionaries/entry/{entry['id']}").json['entry']
            self.assertEqual([b['text'] for b in complete['blocks']],[b['text'] for b in parsed[group['source']]['entries'][0]['blocks']])
            # Existing saved definition links remain readable, although search
            # now presents only complete articles.
            with closing(sqlite3.connect(self.path)) as conn:
                sense=conn.execute('SELECT id FROM bible_dictionary_senses WHERE entry_id=? ORDER BY block_start LIMIT 1',(entry['id'],)).fetchone()[0]
            data=self.client.get(f"{self.base}/api/dictionaries/entry/{entry['id']}?sense={sense}").json['entry']
            self.assertTrue(data['context_blocks'])
            self.assertEqual(data['context_blocks'][0]['text'],'Old Testament' if group['source']=='mounce' else 'A. Verbs.')
            if group['source']=='vine':
                link=data['blocks'][0]['dictionary_links'][0]
                self.assertEqual(link['headword'],'Edify')
                self.assertEqual(self.client.get(f"{self.base}/api/dictionaries/entry/{link['entry_ids'][0]}").json['entry']['headword'],'EDIFICATION')
        corrupt=copy.deepcopy(parsed);corrupt['mounce']['entries'][0]['blocks'][1]['presentation']['indent_percent']=7
        corrupt['vine']['entries'][0]['blocks'][-1]['text']+=' Changed source words.'
        with self.assertRaises(self.bible.SyncError):self.bible._publish_dictionary_sync(self.path,selected,corrupt,'fixture_folder_id',[],check_preservation=True)
        with closing(sqlite3.connect(self.path)) as conn:self.assertEqual(conn.execute('SELECT id,content_json FROM bible_dictionary_entries ORDER BY id').fetchall(),before)
        self.assertEqual(self.bible._publish_dictionary_sync(self.path,selected,parsed,'fixture_folder_id',[])['formatting_entries'],0)

    def test_dictionary_search_is_source_specific_and_assets_require_login(self):
        self.preview(initialize=True)
        self.publish_fixture()
        self.publish_dictionaries()
        results = self.client.get(self.base + '/api/dictionaries/search?q=alpha&part=all').json
        self.assertEqual([source['label'] for source in results['sources']], ['HarperCollins','Vine','Mounce'])
        self.assertEqual([len(source['entries']) for source in results['sources']], [0,2,3])
        headword = results['sources'][2]['entries'][0]
        full = self.client.get(f"{self.base}/api/dictionaries/entry/{headword['id']}").json['entry']
        self.assertEqual(full['source'], 'mounce')
        self.assertTrue(full['blocks'][1]['references'])
        assets = [run['asset'] for block in full['blocks'] for run in block['runs'] if run['type']=='image']
        response = self.client.get(self.base+'/api/dictionaries/asset/'+assets[0])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, 'image/gif')
        for query in ['H20','G20','GK 10','GK Hebrew 10','GK Greek 10']:
            response = self.client.get(self.base+'/api/dictionaries/search', query_string={'q':query})
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json['sources'][2]['entries'])
            if query.startswith('GK'):
                self.assertEqual(response.json['sources'][1]['entries'], [])
        self.assertEqual(self.client.get(self.base+'/api/dictionaries/search?q=alpha&source=unknown').status_code, 400)
        self.assertTrue(all(not source['entries'] for source in self.client.get(self.base+'/api/dictionaries/search',query_string={'q':"' OR 1=1 --"}).json['sources']))
        with self.client.session_transaction() as account:
            account.clear()
        self.assertEqual(self.client.get(self.base+'/api/dictionaries/asset/'+assets[0]).status_code, 401)

    def step_fixture(self, broken=False):
        """Small source-format archive; full coverage thresholds remain enabled outside tests."""
        import zipfile
        target = Path(self.directory.name) / 'step.zip'
        hebrew = [
            ['H0020','H0020 =','H0020','אב','av','H:N','Alpha','Synthetic Hebrew definition.'],
            ['H3091','H3091G =','H3091G','יהושע','yehoshua','N:N-M-P','Joshua','Synthetic Joshua definition.'],
            ['H0002','H0002 = in Aramaic of','H0001G','אב','av','A:N-M','father','Synthetic Aramaic definition.']]
        greek = [
            ['G0020','G0020 =','G0020','ἀγαλλίασις','agalliasis','G:N','Alpha','Synthetic Greek definition.'],
            ['G2424','G2424I = the Greek of','H3091G','Ἰησοῦς','Iēsous','N:N-M-P','Joshua','Synthetic Joshua definition.'],
            ['G0025','G0025 =','G0025','ἀγαπάω','agapaō','G:V','love','Synthetic love definition.'],
            ['G2199','G2199H =','G2199H','','Zebedaios','N:N-M-P','wife of Zebedee','Synthetic source with no written lemma.']]
        rows = [[] for _ in range(6)]
        for code, book in self.bible.STEP_BOOK_IDS.items():
            if book <= 39:
                part = 0 if book <= 5 else 1 if book <= 17 else 2 if book <= 22 else 3
                rows[part].append([f'{code}.1.1#01=L','אב','av','Alpha','{H0020}','HNcmsa','','','','','',''])
            else:
                rows[4 if book<=43 else 5].append([f'{code}.1.1#01=NKO','λόγος (logos)','Alpha','G0020=N-NSM','ἀγαλλίασις=Alpha','NA28','',''])
        rows[1].append(['Jos.1.1#10=L','יהושע','yehoshua','Joshua','{H3091G}','HNpm','','','','','',''])
        rows[1].append(['Ezr.4.8#01=L','אב','av','father','{H0002}','ANcmsa','','','','','',''])
        rows[1].append(['1Sa.1.1#02=Q(K)','','[ ]','[ ]','','','','','','','',''])
        rows[0].append(['Gen.21.7#05=L','אב','av','nursing','{H3243G}','HNcmsa','','','','','',''])
        rows[4].append(['Jhn.3.16#03=NKO','ἠγάπησεν (ēgapēsen)','loved','G0025=V-AAI-3S','ἀγαπάω=to love','NA28','',''])
        rows[4].append(['Mat.15.6{15.5}#01=NKO','λόγος (logos)','Alpha','G0020=N-NSM','ἀγαλλίασις=Alpha','NA28','',''])
        rows[5].append(['Act.2.11[2.10]#01=NKO','λόγος (logos)','Alpha','G0020=N-NSM','ἀγαλλίασις=Alpha','NA28','',''])
        rows[5].append(['Heb.4.8#04=NKO','Ἰησοῦς (Iēsous)','Joshua','G2424I=N-NSM-P','Ἰησοῦς=Jesus/Joshua','NA28','',''])
        if broken:
            rows[5].pop()
        with zipfile.ZipFile(target, 'w') as archive:
            for name, content in zip(self.bible.STEP_FILES, [hebrew,greek,*rows]):
                archive.writestr('STEPBible-Data-master/'+name, '\n'.join('\t'.join(row) for row in content))
        return target

    def import_step_fixture(self):
        archive = self.step_fixture()
        with patch.object(self.bible,'STEP_MIN_VERSES',(39,27)):
            return self.bible.import_step_archive(self.path,archive)

    def test_step_import_preserves_niv_and_personal_rows_and_is_repeatable(self):
        self.preview(initialize=True)
        self.publish_fixture()
        self.publish_dictionaries()
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("INSERT INTO bible_reader_state(user_key,updated_at) VALUES('pastor:keep','now')")
            verse = conn.execute('SELECT id FROM bible_verses LIMIT 1').fetchone()[0]
            conn.execute("INSERT INTO bible_bookmarks VALUES('keep','pastor:keep',?,0,'Keep','now','now')",(verse,))
            conn.execute("INSERT INTO bible_annotations VALUES('keep','pastor:keep',?,'group','note',0,0,'',NULL,'Keep note',1,'now','now')",(verse,))
            before = self.bible._step_preserved_state(conn)
        archive = self.step_fixture()
        with patch.object(self.bible,'STEP_MIN_VERSES',(39,27)):
            dry = self.bible.import_step_archive(self.path,archive,dry_run=True)
            self.assertFalse(dry['applied'])
            with closing(sqlite3.connect(self.path)) as conn:
                self.assertEqual(self.bible._step_preserved_state(conn,before),before)
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_original_tokens').fetchone()[0],0)
            report = self.bible.import_step_archive(self.path,archive)
            repeated = self.bible.import_step_archive(self.path,archive)
        self.assertEqual(report['counts'],repeated['counts'])
        self.assertEqual(repeated['status'],'unchanged')
        self.assertTrue(Path(report['backup']).is_file())
        with closing(sqlite3.connect(report['backup'])) as backup:
            self.assertEqual(backup.execute('SELECT COUNT(*) FROM bible_original_tokens').fetchone()[0],0)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(self.bible._step_preserved_state(conn,before),before)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_original_versions').fetchone()[0],1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_niv_alignments').fetchone()[0],0)
            self.assertEqual(conn.execute('PRAGMA integrity_check').fetchone()[0],'ok')
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_step_required_passages_identifiers_morphology_and_dictionary_links(self):
        self.preview(initialize=True)
        self.publish_dictionaries()
        report = self.import_step_fixture()
        self.assertGreater(report['counts']['dictionary_links'],0)
        self.assertEqual(report['counts']['omission_markers'],1)
        self.assertEqual(report['counts']['identifiers_without_lexeme'],1)
        with closing(sqlite3.connect(self.path)) as conn:
            for book,chapter,verse,strong,extended,lemma in [(6,1,1,'H3091','H3091G','יהושע'),
                    (58,4,8,'G2424','G2424I','Ἰησοῦς'),(43,3,16,'G25','G0025','ἀγαπάω')]:
                row=conn.execute('''SELECT i.extended_strong,t.lemma,t.spelling,t.transliteration,t.morphology
                    FROM bible_original_tokens t JOIN bible_original_token_identifiers i ON i.token_id=t.id
                    WHERE t.book_id=? AND t.chapter=? AND t.verse=? AND i.standard_strong=?''',(book,chapter,verse,strong)).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row[0],extended)
                self.assertEqual(row[1],lemma)
                self.assertTrue(all(row[2:]))
            self.assertEqual(conn.execute("SELECT language FROM bible_original_tokens WHERE book_id=15 AND chapter=4").fetchone()[0],'Aramaic')
            # The unified identifier for Greek Joshua is Hebrew; it MUST NOT replace G2424.
            self.assertEqual(conn.execute("SELECT standard_strong,unified_strong FROM bible_original_lexemes WHERE extended_strong='G2424I'").fetchone(),('G2424','H3091G'))
            self.assertEqual(conn.execute("SELECT standard_strong,lexeme_id FROM bible_original_token_identifiers WHERE extended_strong='H3243G'").fetchone(),('H3243',None))
            self.assertEqual(conn.execute("SELECT lemma FROM bible_original_lexemes WHERE extended_strong='G2199H'").fetchone(),('',))
            self.assertTrue(conn.execute("SELECT 1 FROM bible_original_tokens WHERE source_ref='Act.2.11[2.10]#01=NKO'").fetchone())
            self.assertTrue(conn.execute('''SELECT 1 FROM bible_original_dictionary_links x
                JOIN bible_original_lexemes l ON l.id=x.lexeme_id WHERE l.extended_strong='G0020' ''').fetchone())

    def test_step_invalid_archive_never_migrates_or_modifies_live_database(self):
        self.preview(initialize=True)
        archive=self.step_fixture(broken=True)
        before=self.path.read_bytes()
        with patch.object(self.bible,'STEP_MIN_VERSES',(39,27)):
            with self.assertRaises(self.bible.SyncError):
                self.bible.import_step_archive(self.path,archive)
        self.assertEqual(self.path.read_bytes(),before)
        self.assertFalse((Path(self.directory.name)/'bible_study_backups').exists())

    def test_step_verified_alignment_only_and_stale_or_wrong_verse_links_fail_closed(self):
        self.preview(initialize=True)
        self.publish_fixture()
        self.publish_dictionaries()
        self.import_step_fixture()
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.row_factory=sqlite3.Row
            niv=conn.execute("SELECT v.* FROM bible_verses v JOIN bible_editions e ON e.id=v.edition_id WHERE e.translation='NIV'").fetchone()
            tokens=conn.execute('SELECT id FROM bible_tokens WHERE verse_id=? ORDER BY token_index',(niv['id'],)).fetchall()
            original=conn.execute('SELECT id FROM bible_original_tokens WHERE book_id=1 AND chapter=1 AND verse=1').fetchone()[0]
            source=conn.execute('SELECT source_id FROM bible_original_versions').fetchone()[0]
        url=f"{self.base}/api/word/{niv['id']}/0"
        with patch.object(self.bible,'search_dictionaries',wraps=self.bible.search_dictionaries) as lookup:
            result=self.client.get(url).json
            lookup.assert_called_once()
        self.assertFalse(result['verified'])
        self.assertEqual(result['result_type'],'dictionary')
        self.assertNotIn('original_tokens',result)
        self.assertIn('not a verified NIV word alignment',result['explanation'])
        with closing(sqlite3.connect(self.path)) as conn, conn:
            # A synthetic curated phrase alignment covers two NIV tokens and one source token.
            alignment=conn.execute('''INSERT INTO bible_niv_alignments(verse_id,verse_content_hash,status,evidence_source_id,
                evidence_locator,verification_method,verified_by,verified_at) VALUES(?,?,'verified',?,'fixture:1','curated_verified','reviewer','now')''',
                (niv['id'],niv['content_hash'],source)).lastrowid
            conn.executemany('INSERT INTO bible_niv_alignment_tokens VALUES(?,?)',[(alignment,t[0]) for t in tokens])
            conn.execute('INSERT INTO bible_niv_alignment_originals VALUES(?,?)',(alignment,original))
        result=self.client.get(url).json
        self.assertTrue(result['verified'])
        self.assertEqual(result['original_tokens'][0]['id'],original)
        self.assertEqual([g['label'] for g in result['sources']],['HarperCollins','Vine','Mounce'])
        self.assertTrue(self.client.get(url.rsplit('/',1)[0]+'/1').json['verified'])
        for sql in ("UPDATE bible_niv_alignments SET verse_content_hash='"+'f'*64+"'",
                    "UPDATE bible_niv_alignments SET status='pending'",
                    "UPDATE bible_original_tokens SET verse=2 WHERE id="+str(original)):
            with closing(sqlite3.connect(self.path)) as conn, conn:
                conn.execute(sql)
            self.assertFalse(self.client.get(url).json['verified'])
            with closing(sqlite3.connect(self.path)) as conn, conn:
                conn.execute("UPDATE bible_niv_alignments SET verse_content_hash=?,status='verified'",(niv['content_hash'],))
                conn.execute('UPDATE bible_original_tokens SET verse=1 WHERE id=?',(original,))
        with closing(sqlite3.connect(self.path)) as conn, conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute('''INSERT INTO bible_niv_alignments(verse_id,verse_content_hash,status,verification_method)
                    VALUES(?,?,'verified','curated_verified')''',(niv['id'],niv['content_hash']))
            foreign_verse=conn.execute('SELECT id FROM bible_verses WHERE id!=? LIMIT 1',(niv['id'],)).fetchone()[0]
            foreign_token=conn.execute("INSERT INTO bible_tokens(verse_id,token_index,start_offset,end_offset,text) VALUES(?,0,0,5,'Alpha')",(foreign_verse,)).lastrowid
            conn.execute('INSERT INTO bible_niv_alignment_tokens VALUES(?,?)',(alignment,foreign_token))
        self.assertFalse(self.client.get(url).json['verified'])
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute('DELETE FROM bible_niv_alignment_tokens WHERE token_id=?',(foreign_token,))
            conn.execute("UPDATE bible_verses SET text='Changed without a matching content hash' WHERE id=?",(niv['id'],))
        self.assertFalse(self.client.get(url).json['verified'])
        page=self.client.get(self.base).get_data(as_text=True)
        self.assertNotIn('bsConcordanceHeading',page)
        self.assertNotIn('Click any NIV word to search both books',page)

    def test_v2_migration_preserves_records_and_rolls_back_on_failure(self):
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.executescript(self.bible.SCHEMA_SQL+self.bible.DICTIONARY_SCHEMA_SQL)
            conn.execute(f'PRAGMA application_id={self.bible.APPLICATION_ID}')
            conn.execute('PRAGMA user_version=2')
            conn.execute("INSERT INTO bible_metadata VALUES('keep','unchanged')")
        with patch.object(self.bible,'ORIGINAL_SCHEMA_SQL',self.bible.ORIGINAL_SCHEMA_SQL+';INVALID SQL'):
            with self.assertRaises(sqlite3.DatabaseError):
                self.bible.initialize_database(self.path)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0],2)
            self.assertFalse(conn.execute("SELECT name FROM sqlite_master WHERE name='bible_original_tokens'").fetchone())
        self.bible.initialize_database(self.path)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT * FROM bible_metadata').fetchall(),[('keep','unchanged')])

    def test_v1_migration_is_additive_and_rolls_back_on_failure(self):
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.executescript(self.bible.SCHEMA_SQL)
            conn.executemany('INSERT INTO bible_books(id,name,chapter_count) VALUES(?,?,?)',
                             [(index,name,count) for index,(name,count) in enumerate(self.bible.BOOKS,1)])
            conn.execute(f'PRAGMA application_id={self.bible.APPLICATION_ID}')
            conn.execute('PRAGMA user_version=1')
            conn.execute("INSERT INTO bible_reader_state(user_key,updated_at,progress_percent) VALUES('pastor:fixture','now',45)")
        self.publish_fixture()
        with closing(sqlite3.connect(self.path)) as conn:
            before = conn.execute('SELECT * FROM bible_reader_state').fetchall()
            original_verses = conn.execute('SELECT * FROM bible_verses').fetchall()
        self.assertEqual(self.bible.get_indexed_niv_passage('Genesis 1:1',self.path),'Alpha 😀 beta.')
        with patch.object(self.bible,'DICTIONARY_SCHEMA_SQL',self.bible.DICTIONARY_SCHEMA_SQL+';INVALID SQL'):
            with self.assertRaises(sqlite3.DatabaseError):
                self.bible.initialize_database(self.path)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0],1)
            self.assertEqual(conn.execute("SELECT name FROM sqlite_master WHERE name='bible_dictionary_entries'").fetchall(),[])
        self.bible.initialize_database(self.path)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0],self.bible.SCHEMA_VERSION)
            self.assertEqual(conn.execute('SELECT * FROM bible_reader_state').fetchall(),before)
            self.assertEqual(conn.execute('SELECT * FROM bible_verses').fetchall(),original_verses)

    def test_scripture_citations_distinguish_books_and_preserve_ranges(self):
        text='😀 John 3:16; 18:2. 1 John 3:16-18. Matt. 5:3-5; 6:1, 2. Jude 16.'
        refs=self.bible.scripture_references(text)
        self.assertEqual([(item['book'],item['chapter'],item['verse']) for item in refs],
                         [('John',3,16),('John',18,2),('1 John',3,16),('Matthew',5,3),('Matthew',6,1),('Matthew',6,2),('Jude',1,16)])
        for item in refs:
            self.assertEqual(text[item['start']:item['end']],item['source_text'])
        self.assertEqual(self.bible.scripture_references('GK 6440 (S 5800), 214x.'),[])
        self.assertEqual(self.bible.scripture_references('John 3:16–4:2')[0]['end_chapter'],4)
        self.assertEqual(self.bible.scripture_references('John 3:16a–17b')[0]['end_verse'],17)

    def test_direct_drive_discovery_never_needs_the_library_catalog(self):
        drive=Mock()
        folder=dict(id='fixture_folder_id',name='For Scripture Study App',mimeType=resources.GOOGLE_DRIVE_FOLDER_MIME)
        files=[dict(id='fixture_niv_id',name='Holy Bible (NIV).epub',modifiedTime='now'),
               dict(id='fixture_mounce_id',name='Mounces Complete Expository Dictionary.epub',modifiedTime='now'),
               dict(id='fixture_vine_id',name='Vines Complete Expository Dictionary.epub',modifiedTime='now'),
               dict(id='fixture_old_id',name='Ultimate Bible Study Suite.epub')]
        with patch.object(resources,'list_drive_folder',side_effect=[[folder],files]) as listing, \
             patch.object(self.bible,'_library_sources',side_effect=AssertionError('No catalog dependency')):
            folder_id,sources,ignored=self.bible._official_drive_sources(drive)
        self.assertEqual(folder_id,'fixture_folder_id')
        self.assertEqual(set(sources),{'NIV','mounce','vine'})
        self.assertEqual(ignored,['Ultimate Bible Study Suite.epub'])
        self.assertEqual(listing.call_args.args[1],'fixture_folder_id')
        with patch.object(resources,'list_drive_folder',return_value=files+[files[0]]):
            with self.assertRaises(self.bible.SyncError):
                self.bible._official_drive_sources(drive,'fixture_folder_id')

    def maxwell_index_fixture(self):
        path=Path(self.directory.name)/'ai_index.db'
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute('CREATE TABLE pij_library_documents(id INTEGER,title TEXT,source_type TEXT,searchable INTEGER,checksum TEXT,modified_time TEXT,indexed_at TEXT,chunk_count INTEGER)')
            conn.execute("INSERT INTO pij_library_documents VALUES(1,'NIV, Maxwell Leadership Bible, 3rd Edition','public_ebook',1,'hash','now','now',1)")
            conn.execute('CREATE TABLE pij_library_chunks(document_id INTEGER,chunk_number INTEGER,page_start INTEGER,content TEXT)')
            conn.execute('INSERT INTO pij_library_chunks VALUES(1,1,1,?)',
                         ('Leadership with responsibility\nGalatians 6:1–6\nLeaders bear responsibility and help others with their burdens.\nGo to the Index of Leadership Articles .\n'
                          'Love in another book\n1 John 3:16\nA different leadership discussion about love.\nGo to the Index of Leadership Articles .',))
        return path

    def test_thompson_pdf_normalization_preserves_colors_without_hyphen_damage(self):
        value,spans=self.bible._thompson_verse_text([(c,True) for c in 'Je\x02\r\nsus said, seed-bearing.']+[(c,False) for c in ' Amen.'])
        self.assertEqual(value,'Jesus said, seed-bearing. Amen.')
        self.assertEqual(value[spans[0][0]:spans[0][1]],'Jesus said, seed-bearing.')
        with self.assertRaises(Exception):self.bible.parse_thompson_pdf(b'not a PDF')

    def test_thompson_concordance_inherits_printed_references_and_keeps_short_excerpts(self):
        lines = [(1968,True,'ABILITY (ABLE)'),(1968,False,'Ezr 2: 69 According to their a they gave'),
                 (1968,False,'2Co 1: 8 far beyond our a to endure,'),(1968,False,'8: 3 even beyond their a.'),
                 (1968,True,'FAITH (FAITHFUL'),(1968,False,'FAITHFULLY)'),
                 (1968,False,'Jude : 2 be yours in a.'),(1968,False,': 3 another short excerpt.'),
                 (1969,True,'AARON'),(1969,False,'Priesthood (Heb 5:1–4; 7).'),
                 (1969,True,'PARENT'),(1969,False,'Pr 1: 8 Listen to your father.'),
                 (1969,True,'PARENTS'),(1969,False,'15: 5 A fool spurns discipline.'),
                 (1969,True,'TALENT See BAGS')]
        entries=self.bible._thompson_concordance_entries(lines)
        ability,faith,aaron,_,parents,alias=entries
        refs=[b['references'][0] for b in ability['blocks']]
        self.assertEqual([(r['book'],r['chapter'],r['verse']) for r in refs],
                         [('Ezra',2,69),('2 Corinthians',1,8),('2 Corinthians',8,3)])
        self.assertEqual(ability['related'],['ABLE'])
        self.assertEqual(faith['related'],['FAITHFUL','FAITHFULLY'])
        self.assertEqual([b['references'][0]['chapter'] for b in faith['blocks']],[1,1])
        self.assertEqual(parents['blocks'][0]['references'][0]['book'],'Proverbs')
        self.assertEqual([r['source_text'] for r in aaron['blocks'][0]['references']],['Heb 5:1–4'])
        self.assertEqual((alias['headword'],alias['related']),('TALENT',['BAGS']))
        for entry in entries:
            for block in entry['blocks']:
                for ref in block['references']:
                    self.assertEqual(block['text'][ref['start']:ref['end']],ref['source_text'])

    def thompson_study_fixture(self):
        self.preview(initialize=True);self.publish_fixture()
        entries=self.bible._thompson_concordance_entries([
            (2000,True,'JOHN'),(2000,False,'1. Son of Zechariah. Called the Baptist (Jn 1:6–8). 2. Apostle; brother of James (Mt 4:21–22).'),
            (2001,True,'MARY'),(2001,False,'1. Mother of Jesus (Lk 1:27–56). 2. Magdalene (Mk 15:40).'),
            (2002,True,'NOAH'),(2002,False,'Righteous man (Ge 6:9).'),
            (2002,True,'JUDAH'),(2002,False,'1. Son of Jacob (Ge 29:35). 2. Name used for people and land of Southern Kingdom.')])
        book_text='KEY VERSE: 3:15. See Acts 7:30.'
        material=dict(studies=[dict(key='book:1',kind='book',title='Genesis',book_id=1,aliases=[],source_page=22,
            blocks=[dict(type='paragraph',text=book_text,references=self.bible._study_references(book_text,1))]),
            dict(key='character:4286',kind='character',title='Noah, the Ark Builder',book_id=None,aliases=['Noah'],source_page=1893,
                 blocks=[dict(type='paragraph',text='Noah walked with God. Genesis 6:9.',references=self.bible.scripture_references('Noah walked with God. Genesis 6:9.'))]),
            dict(key='charts:Noah',kind='chart',title='Noah source chart',book_id=None,aliases=['Noah'],source_page=1893,blocks=[],pages=[1893])],
            classifications=[dict(name='Noah',text='Noah, “comfort,” the ark builder.',category='Patriarchs',source_page=1903),
                dict(name='Mary',text='Mary, mother of James.',category='Women',source_page=1905)],images={1893:b'\x89PNG\r\n\x1a\nfixture'})
        with closing(sqlite3.connect(self.path)) as conn,conn:
            edition=conn.execute("SELECT id FROM bible_editions WHERE is_active=1 AND translation='NIV'").fetchone()[0]
            self.bible._publish_thompson_concordance(conn,edition,entries)
            self.bible._publish_thompson_studies(conn,edition,material)
        return material,edition

    def test_book_study_and_people_use_source_references_and_explicit_choices(self):
        material,edition=self.thompson_study_fixture()
        result=self.client.get(self.base+'/api/book-study?book=Genesis').json
        self.assertTrue(result['available'])
        refs=result['study']['blocks'][0]['references']
        self.assertEqual([(r['book'],r['chapter'],r['verse']) for r in refs],[('Genesis',3,15),('Acts',7,30)])
        for ref in refs:self.assertEqual(result['study']['blocks'][0]['text'][ref['start']:ref['end']],ref['source_text'])
        self.assertEqual(self.client.get(self.base+'/api/book-study?book=wrong').status_code,400)
        john=self.client.get(self.base+'/api/dictionaries/search?q=John').json['people']
        self.assertEqual(len(john['people']),2);self.assertIsNone(john['selected'])
        with closing(sqlite3.connect(self.path)) as conn:
            conn.row_factory=sqlite3.Row
            selected=self.bible.person_study(conn,'John',(43,1,6))['selected']
            self.assertIn('Zechariah',selected['label'])
            self.assertIsNone(self.bible.person_study(conn,'John',(1,1,1))['selected'])
            mary=self.bible.person_study(conn,'Mary')
            self.assertEqual(len(mary['people']),3)
            self.assertTrue(any('mother of James' in p['label'] for p in mary['people']))
            judah=self.bible.person_study(conn,'Judah')
            self.assertEqual(len(judah['people']),1);self.assertIsNone(judah['selected'])
            self.assertIn('Jacob',self.bible.person_study(conn,'Judah',(1,29,35))['selected']['label'])
        noah_data=self.client.get(self.base+'/api/dictionaries/search?q=Noah').json
        self.assertEqual(noah_data['word_type']['part'],'name')
        noah=noah_data['people']['selected']
        self.assertTrue(any('Character Studies' in section['title'] for section in noah['sections']))
        self.assertTrue(any('Classified' in section['title'] for section in noah['sections']))
        self.assertTrue(any(section.get('pages')==[1893] for section in noah['sections']))
        self.assertEqual(self.client.get(self.base+'/api/people/'+str(noah['id'])).json['person']['id'],noah['id'])
        image=self.client.get(self.base+'/api/studies/page/1893')
        self.assertEqual(image.mimetype,'image/png')
        self.assertEqual(self.client.get(self.base+'/api/studies/page/9999').status_code,404)
        with closing(sqlite3.connect(self.path)) as conn,conn:conn.execute('UPDATE bible_editions SET is_active=0 WHERE id=?',(edition,))
        self.assertFalse(self.client.get(self.base+'/api/book-study?book=Genesis').json['available'])
        self.assertEqual(self.client.get(self.base+'/api/people/'+str(noah['id'])).status_code,404)
        self.assertEqual(self.client.get(self.base+'/api/studies/page/1893').status_code,404)
        with self.client.session_transaction() as session:session.clear()
        for url in ['/api/book-study?book=Genesis','/api/studies/page/1893','/api/people/1']:
            self.assertEqual(self.client.get(self.base+url).status_code,401)

    def test_v5_study_migration_is_additive_and_rolls_back_when_invalid(self):
        self.preview(initialize=True);self.publish_fixture()
        with closing(sqlite3.connect(self.path)) as conn,conn:
            for table in ('bible_thompson_studies','bible_thompson_study_pages','bible_person_profiles'):conn.execute('DROP TABLE '+table)
            conn.execute('PRAGMA user_version=5')
            before=conn.execute('SELECT * FROM bible_verses').fetchall()
        self.assertFalse(self.client.get(self.base+'/api/book-study?book=Genesis').json['available'])
        with patch.object(self.bible,'STUDY_SCHEMA_SQL',self.bible.STUDY_SCHEMA_SQL+';INVALID SQL'):
            with self.assertRaises(sqlite3.DatabaseError):self.bible.initialize_database(self.path)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0],5)
            self.assertFalse(conn.execute("SELECT 1 FROM sqlite_master WHERE name='bible_person_profiles'").fetchone())
        self.bible.initialize_database(self.path)
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT * FROM bible_verses').fetchall(),before)
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_thompson_concordance_import_migrates_v4_preserves_records_and_is_repeatable(self):
        import hashlib,json
        self.preview(initialize=True);self.publish_fixture()
        data=b'%PDF-concordance-fixture'
        pdf=Path(self.directory.name)/'thompson.pdf';pdf.write_bytes(data)
        source=dict(title='Thompson fixture',drive_file_id='fixture_thompson_id',
                    sha256=hashlib.sha256(data).hexdigest(),fingerprint={'modifiedTime':'now'})
        parsed=dict(thompson=True,edition_label='Thompson',verses={(1,1,1):dict(text='Fixture text.',heading='',source_page=23)},topics={})
        self.bible._publish_dictionary_sync(self.path,{'NIV':source},{'NIV':parsed},'fixture_folder_id',[])
        with closing(sqlite3.connect(self.path)) as conn,conn:
            verse=conn.execute('SELECT id FROM bible_verses ORDER BY id LIMIT 1').fetchone()[0]
            conn.execute("INSERT INTO bible_bookmarks VALUES('keep','pastor:keep',?,0,'Keep','now','now')",(verse,))
            conn.execute("INSERT INTO bible_annotations VALUES('keep','pastor:keep',?,'group','note',0,0,'',NULL,'Keep note',1,'now','now')",(verse,))
            for table in ('bible_thompson_concordance','bible_thompson_studies','bible_thompson_study_pages','bible_person_profiles'):
                conn.execute('DROP TABLE '+table)
            conn.execute('PRAGMA user_version=4')
            before={t:conn.execute('SELECT * FROM '+t).fetchall() for t in
                ('bible_editions','bible_verses','bible_tokens','bible_reader_state','bible_chain_topics','bible_dictionary_entries',
                 'bible_bookmarks','bible_annotations')}
        self.assertEqual(self.client.get(self.base+'/api/status').json['status'],'schema_ready')
        self.assertFalse(self.client.get(self.base+'/api/concordance?q=love').json['available'])
        entries=self.bible._thompson_concordance_entries([
            (2002,True,'LOVE (LOVED LOVING)'),(2002,False,'Jn 3: 16 For God so l the world'),
            (2003,True,'LOVED (LOVE)'),(2003,False,'Jn 3: 16 For God so l the world')])
        with patch('pypdfium2.PdfDocument'),patch.object(self.bible,'_parse_thompson_concordance',return_value=entries):
            self.assertTrue(self.bible.import_thompson_concordance(self.path,pdf,dry_run=True)['dry_run'])
            with closing(sqlite3.connect(self.path)) as conn:self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0],4)
            report=self.bible.import_thompson_concordance(self.path,pdf)
            self.assertTrue(Path(report['backup']).is_file())
            self.bible.import_thompson_concordance(self.path,pdf)
            with closing(sqlite3.connect(self.path)) as conn:
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_thompson_concordance').fetchone()[0],2)
                for table,rows in before.items():self.assertEqual(conn.execute('SELECT * FROM '+table).fetchall(),rows)
                self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(),[])
            exact=self.client.get(self.base+'/api/concordance?q=LOVED').json
            self.assertEqual([e['headword'] for e in exact['entries']],['LOVED'])
            fallback=self.client.get(self.base+'/api/concordance?q=loving').json
            self.assertEqual([e['headword'] for e in fallback['entries']],['LOVE'])
            self.assertEqual(exact['entries'][0]['blocks'][0]['text'],'John 3:16 For God so l the world')
            self.assertEqual(self.client.get(self.base+'/api/concordance?q=unlisted').json['entries'],[])
            for query in ('?q=love&offset=-1','?q=love&offset=bad','?q='+'a'*101):
                self.assertEqual(self.client.get(self.base+'/api/concordance'+query).status_code,400)
            pdf.write_bytes(b'mismatched source')
            with self.assertRaises(self.bible.SyncError):self.bible.import_thompson_concordance(self.path,pdf)
        with self.client.session_transaction() as session:session.clear()
        self.assertEqual(self.client.get(self.base+'/api/concordance?q=love').status_code,401)

    def test_unchanged_thompson_sync_adds_missing_concordance_without_replacing_verses(self):
        import hashlib,copy
        self.preview(initialize=True);self.publish_fixture()
        data=b'%PDF-sync-concordance-fixture'
        source=dict(title='Thompson fixture',drive_file_id='fixture_thompson_id',thompson=True,
                    sha256=hashlib.sha256(data).hexdigest(),fingerprint={'modifiedTime':'now'})
        parsed=dict(thompson=True,edition_label='Thompson',verses={(1,1,1):dict(text='Fixture text.',heading='',source_page=23)},topics={})
        self.bible._publish_dictionary_sync(self.path,{'NIV':source},{'NIV':parsed},'fixture_folder_id',[])
        with closing(sqlite3.connect(self.path)) as conn:
            before=conn.execute('SELECT * FROM bible_verses').fetchall()
            edition_count=conn.execute('SELECT COUNT(*) FROM bible_editions').fetchone()[0]
        parsed['concordance']=self.bible._thompson_concordance_entries([(2002,True,'LOVE'),(2002,False,'Jn 3: 16 For God so l')])
        parsed['studies']=dict(studies=[dict(key='book:1',kind='book',title='Genesis',book_id=1,aliases=[],blocks=[],source_page=22,
            outline=dict(blocks=[dict(type='paragraph',text='Exact source outline.')],source_page=1888))],classifications=[],images={})
        with patch.object(self.bible,'_official_drive_sources',return_value=('fixture_folder_id',{'NIV':copy.deepcopy(source)},[])), \
             patch.object(self.bible,'parse_thompson_pdf',return_value=parsed), \
             patch.object(self.bible,'_validate_niv_source'):
            calls=[]
            report=self.bible.sync_sources(self.path,'fixture_folder_id',lambda _:None,fetch=lambda item:calls.append(item) or data,drive=Mock())
            self.assertEqual(len(calls),1)
            self.assertEqual(report['counts']['thompson_concordance_entries'],1)
            self.bible.sync_sources(self.path,'fixture_folder_id',lambda _:None,
                fetch=lambda _:self.fail('Already indexed concordance must not download again'),drive=Mock())
            with closing(sqlite3.connect(self.path)) as conn,conn:
                conn.execute("UPDATE bible_thompson_studies SET content_json='{}'")
            calls=[]
            self.bible.sync_sources(self.path,'fixture_folder_id',lambda _:None,fetch=lambda item:calls.append(item) or data,drive=Mock())
            self.assertEqual(len(calls),1,'Missing inline outline must refresh an otherwise unchanged source')
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT * FROM bible_verses').fetchall(),before)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM bible_editions').fetchone()[0],edition_count)

    def test_related_chain_links_resolve_only_source_topic_keys_in_same_edition(self):
        import json
        self.preview(initialize=True);self.publish_fixture()
        with closing(sqlite3.connect(self.path)) as conn,conn:
            edition=conn.execute("SELECT id FROM bible_editions WHERE is_active=1 AND translation='NIV'").fetchone()[0]
            topic_ids={}
            for key in ('1424','718','1164','1452','1456'):
                text='→ Christ Exalted, 718; Missing, 9999; God Exalted, 1164; Praise, 1452; Thankfulness (2), 1456.'
                blocks=[dict(type='paragraph',text='John 3:16 has 718 words.'),dict(type='paragraph',role='related',text=text)]
                topic_ids[key]=conn.execute('''INSERT INTO bible_chain_topics(edition_id,topic_key,title,content_json,source_page)
                    VALUES(?,?,?,?,1649)''',(edition,key,'Fixture '+key,json.dumps(blocks))).lastrowid
        result=self.client.get(self.base+'/api/chain/'+str(topic_ids['1424'])).json['topic']
        self.assertNotIn('chain_links',result['blocks'][0])
        note=result['blocks'][1]
        self.assertEqual([link['key'] for link in note['chain_links']],['718','1164','1452','1456'])
        self.assertEqual(note['text'][note['chain_links'][1]['start']:note['chain_links'][1]['end']],'God Exalted, 1164')
        self.assertEqual(note['text'][note['chain_links'][-1]['start']:note['chain_links'][-1]['end']],
                         'Thankfulness (2), 1456')

    def test_thompson_index_keeps_sections_examples_and_verse_paragraphs(self):
        def run(text, font='ZNIVSans11-Regular', red=False):
            return [dict(char=char, font=font, red=red) for char in text]
        bold, italic = 'ZNIVSans11-Semibold', 'ZNIVSans11-SemiboldItalic'
        lines = [run('SELF-SACRIFICE (M. Self-sacrifice (2))', 'ZNIVSansClosed11-Semibold'),
                 run('2. Examples', bold),
                 run('a. General References to, ', bold)+run('Ge 42:37;'),
                 run('44:33; Ex 32:32.'),
                 run('b. Selected People', bold),
                 run('Jephthah\u2019s Daughter, ', italic)+run('Jdg 11:36.'),
                 run('Moses', italic),
                 run('Ex 32:32 ', bold, True)+run('But now, please for\x02'),
                 run('give their sin.'),
                 run('Jn 3:16 ', bold, True)+run('For God so loved the world.'),
                 run('\uf0aa Leaving All, 3206; Suffering for'), run('Christ, 3474.')]
        blocks = self.bible._thompson_topic_blocks(lines)
        self.assertEqual([b['role'] for b in blocks],
            ['intro','section','section','references','section','example','subheading','verse','verse','related'])
        refs = blocks[3]['references']
        self.assertEqual([(r['book'],r['chapter'],r['verse']) for r in refs],
                         [('Genesis',42,37),('Genesis',44,33),('Exodus',32,32)])
        self.assertTrue(any('em' in r['marks'] for r in blocks[5]['runs']))
        self.assertEqual(blocks[7]['text'], 'Ex 32:32 But now, please forgive their sin.')
        self.assertEqual(blocks[-1]['text'], '\u2192 Leaving All, 3206; Suffering for Christ, 3474.')
        for block in blocks:
            self.assertEqual(''.join(r['text'] for r in block['runs']),block['text'])
            for ref in block['references']:
                self.assertEqual(block['text'][ref['start']:ref['end']],ref['source_text'])

    def test_thompson_layout_upgrade_preserves_ids_links_and_recovers_from_mismatch(self):
        import hashlib
        import json
        self.preview(initialize=True); self.publish_fixture()
        data = b'%PDF-fixture-for-layout'
        pdf = Path(self.directory.name)/'source.pdf'
        pdf.write_bytes(data)
        text = 'Love. Gen. 1:1.'
        topic = dict(title='Love',text=text,source_page=1540,references=self.bible.scripture_references(text))
        source = dict(title='Thompson fixture',drive_file_id='fixture_thompson_id',
                      sha256=hashlib.sha256(data).hexdigest(),fingerprint={'modifiedTime':'now'})
        parsed = dict(thompson=True,edition_label='Thompson',verses={(1,1,1):
                      dict(text='Fixture verse.',heading='',source_page=23)},topics={'2201':topic})
        self.bible._publish_dictionary_sync(self.path,{'NIV':source},{'NIV':parsed},'fixture_folder_id',[])
        blocks = [dict(type='heading',role='section',text='Love'),
                  dict(type='paragraph',role='references',text='Gen. 1:1.',references=self.bible.scripture_references('Gen. 1:1.'))]
        topic['blocks'] = blocks
        with closing(sqlite3.connect(self.path)) as conn:
            before = {table:conn.execute('SELECT * FROM '+table).fetchall() for table in
                      ('bible_verses','bible_tokens','bible_chain_verses','bible_editions','bible_reader_state')}
            topic_id = conn.execute('SELECT id FROM bible_chain_topics').fetchone()[0]
        with patch('pypdfium2.PdfDocument'), patch.object(self.bible,'_parse_thompson_index',return_value={'2201':topic}):
            report = self.bible.format_thompson_index(self.path,pdf,dry_run=True)
            self.assertTrue(report['dry_run'])
            with closing(sqlite3.connect(self.path)) as conn:
                self.assertNotEqual(json.loads(conn.execute('SELECT content_json FROM bible_chain_topics').fetchone()[0]),blocks)
            report = self.bible.format_thompson_index(self.path,pdf)
            self.assertTrue(Path(report['backup']).is_file())
            result = self.client.get(self.base+'/api/chain/'+str(topic_id)).json['topic']
            self.assertEqual([{k:v for k,v in block.items() if k!='_study_texts'} for block in result['blocks']],blocks)
            with closing(sqlite3.connect(self.path)) as conn:
                for table,rows in before.items():
                    self.assertEqual(conn.execute('SELECT * FROM '+table).fetchall(),rows)
            with patch.object(self.bible,'_parse_thompson_index',return_value={}):
                with self.assertRaises(self.bible.SyncError):self.bible.format_thompson_index(self.path,pdf)
            pdf.write_bytes(b'different PDF')
            with self.assertRaises(self.bible.SyncError):self.bible.format_thompson_index(self.path,pdf)

    def test_dictionary_senses_separate_testaments_and_reject_prose_enumerations(self):
        def block(text,italic=''):
            runs=[dict(type='text',text=text,marks=[])]
            if italic:
                start=text.index(italic);runs=[dict(type='text',text=text[:start],marks=[]),dict(type='text',text=italic,marks=['em']),dict(type='text',text=text[start+len(italic):],marks=[])]
            return dict(type='paragraph',text=text,runs=runs,references=self.bible.scripture_references(text))
        entry=dict(family='mounce',section='Expository dictionary',language='',blocks=[block('Old Testament'),block('Verb: ahab, GK 170 (S 157), 217x.'),block('Old explanation.'),block('New Testament'),block('Verb: agapao, GK 26 (S 25), 143x.'),block('John 3:16.'),block('Noun: agape, GK 27 (S 26), 116x.'),block('Other meaning.')])
        senses=list(self.bible.dictionary_senses(entry))
        self.assertEqual([(s['language'],s['block_start'],s['block_end']) for s in senses],[('Hebrew',1,3),('Greek',4,6),('Greek',6,8)])
        self.assertIn(('strong','Greek',25,'','GK 26 (S 25)'),senses[1]['identifiers'])
        vine=dict(family='vine',section='New Testament',language='Greek',blocks=[block('1. agapao (25) First meaning.','agapao'),block('The word agapao (1) occurs in this discussion.','agapao'),block('2. phileo (5368) Second meaning.','phileo')])
        senses=list(self.bible.dictionary_senses(vine))
        self.assertEqual([(s['block_start'],s['block_end']) for s in senses],[(0,2),(2,3)])
        self.assertEqual([i[2] for s in senses for i in s['identifiers'] if i[0]=='strong'],[25,5368])
        self.assertIn('love',self.bible.english_word_forms('loved'))
        self.assertIn('mocker',self.bible.english_word_forms('mockers'))
        preview=self.bible._dictionary_preview_block(dict(text='A\ufffc long source',runs=[dict(type='text',text='A',marks=[]),dict(type='image',asset='a'*64,marks=[]),dict(type='text',text=' long source',marks=['em'])],references=[]),limit=7)
        self.assertEqual(preview['runs'][1]['type'],'image')
        self.assertEqual(preview['runs'][-1]['text'],'…')
        self.assertEqual(preview['runs'][2]['marks'],['em'])

    def test_thompson_topic_red_letters_and_edition_preserve_old_verse_ids(self):
        self.preview(initialize=True);self.publish_fixture()
        with closing(sqlite3.connect(self.path)) as conn:
            old=conn.execute('SELECT * FROM bible_verses ORDER BY id').fetchall()
        source=dict(title='Thompson fixture',drive_file_id='fixture_thompson_id',sha256='e'*64,fingerprint={'modifiedTime':'now'})
        topic=dict(title='Love',text='Love. Gen. 1:1.',source_page=1540,references=self.bible.scripture_references('Love. Gen. 1:1.'))
        parsed=dict(thompson=True,edition_label='NIV 2011 · Thompson',attribution='Fixture attribution',verses={(1,1,1):dict(text='New edition text.',heading='New heading',red_spans=[],source_page=23),(40,5,44):dict(text='Love your enemies.',heading='',red_spans=[(0,18)],source_page=1158)},topics={'2201':topic})
        self.bible._publish_dictionary_sync(self.path,{'NIV':source},{'NIV':parsed},'fixture_folder_id',[])
        chapter=self.client.get(self.base+'/api/chapter').json['verses'][0]
        self.assertEqual(chapter['edition_label'],'NIV 2011 · Thompson')
        self.assertEqual(chapter['topics'][0]['topic_key'],'2201')
        result=self.client.get(self.base+'/api/chain/'+str(chapter['topics'][0]['id'])).json['topic']
        self.assertEqual(result['blocks'][0]['text'],topic['text'])
        self.assertEqual(result['blocks'][0]['references'][0]['book'],'Genesis')
        red=self.client.get(self.base+'/api/chapter?book=Matthew&chapter=5').json['verses'][0]
        self.assertEqual(red['red_spans'],[{'start_offset':0,'end_offset':18}])
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT * FROM bible_verses WHERE id<=? ORDER BY id',(old[-1][0],)).fetchall(),old)
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_history_is_account_owned_csrf_protected_and_deduplicated(self):
        headers=self.preview(initialize=True);self.publish_fixture()
        verse=self.client.get(self.base+'/api/chapter').json['verses'][0]['id']
        endpoint=self.base+'/api/history'
        self.assertEqual(self.client.post(endpoint,json={'verse_id':verse}).status_code,400)
        self.assertEqual(self.client.post(endpoint,json={'verse_id':verse,'user_key':'other'},headers=headers).status_code,400)
        for _ in range(2):self.assertTrue(self.client.post(endpoint,json={'verse_id':verse},headers=headers).json['saved'])
        self.assertEqual(len(self.client.get(endpoint).json['history']),1)
        self.login('Another Pastor',pastor=True)
        self.assertEqual(self.client.get(endpoint+'?user_key=area%20overseer:pijeme').json['history'],[])
        self.login();self.assertEqual(self.client.get(endpoint).json['history'][0]['book'],'Genesis')

    def test_thompson_drive_discovery_prefers_pdf_without_deleting_epub(self):
        pdf=dict(id='fixture_thompson_id',name='NIV, Thompson Chain-Reference Bible (Zondervan).pdf')
        epub=dict(id='fixture_old_niv_id',name='Holy Bible NIV.epub')
        for files in ([pdf,epub],[epub,pdf]):
            with patch.object(resources,'list_drive_folder',return_value=files):
                _,selected,ignored=self.bible._official_drive_sources(Mock(),'fixture_folder_id')
            self.assertTrue(selected['NIV']['thompson']);self.assertEqual(selected['NIV']['drive_file_id'],pdf['id'])
            self.assertIn(epub['name'],ignored)

    def test_failed_temporary_publication_never_touches_live_database(self):
        self.preview(initialize=True);self.publish_fixture()
        before=self.path.read_bytes()
        source=dict(title='Invalid NIV',drive_file_id='fixture_invalid_id',sha256='e'*64,fingerprint={})
        with self.assertRaises(self.bible.SyncError):
            self.bible._safe_publish_sources(self.path,{'NIV':source},{'NIV':dict(verses={})},'fixture_folder_id',[],lambda _:None)
        self.assertEqual(self.path.read_bytes(),before)

    def test_maxwell_matches_article_passage_not_another_books_verse(self):
        path=self.maxwell_index_fixture()
        self.assertEqual(self.bible.find_maxwell_note(path,1,'Galatians 6:2')['title'],'Leadership with responsibility')
        self.assertIsNone(self.bible.find_maxwell_note(path,1,'John 3:16'))
        self.assertIsNone(self.bible.find_maxwell_note(path,1,'Galatians 6:7'))
        self.assertEqual(self.bible.find_maxwell_note(path,1,'1 John 3:16')['passage'],'1 John 3:16')

    def test_wiersbe_commentary_matches_source_ranges_and_keeps_original_continuations(self):
        self.preview(initialize=True)
        path=Path(self.directory.name)/'commentary-index.db'
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute('''CREATE TABLE pij_library_documents(id INTEGER,title TEXT,author TEXT,format TEXT,
                source_type TEXT,searchable INTEGER,checksum TEXT,modified_time TEXT,indexed_at TEXT,chunk_count INTEGER)''')
            conn.execute('CREATE TABLE pij_library_chunks(document_id INTEGER,chunk_number INTEGER,page_start INTEGER,content TEXT)')
            for did,fmt in ((1,'epub'),(2,'pdf')):
                conn.execute("INSERT INTO pij_library_documents VALUES(?, 'Be Complete (Colossians)', 'Warren W. Wiersbe', ?, 'public_ebook',1,'hash','now','now',4)",(did,fmt))
            chunks=[(1,10,'6-Source.html\nChapter Six\nA Source Heading\n(Colossians 2:4–15)\nAn original indexed paragraph with enough text to verify that this is the source article, not a generated explanation.\n'),
                    (2,11,'Be Complete\nContinuation paragraph kept exactly, including this punctuation: “source.”\n'),
                    (3,12,'7-Source.html\nChapter Seven\nAnother Source Heading\n(Colossians 2:16–23)\nA different indexed paragraph about the next passage that must not appear for verse ten.\n'),
                    (4,13,'Notes\nChapter Six\n(Colossians 2:4–15)\nFootnotes are not another commentary chapter.')]
            for ordinal,page,text in chunks:conn.execute('INSERT INTO pij_library_chunks VALUES(1,?,?,?)',(ordinal,page,text))
        self.app.config['AI_DATABASE']=str(path)
        before=path.read_bytes()
        data=self.bible.wiersbe_commentary(str(path),51,2,10)
        self.assertFalse(self.client.get(self.base+'/api/commentary?book=Colossians&chapter=2&verse=10').json['available'])
        self.assertTrue(data['available']);self.assertEqual(len(data['entries']),1)
        entry=data['entries'][0]
        self.assertEqual((entry['title'],entry['passage']),('A Source Heading','Colossians 2:4–15'))
        self.assertIn('Continuation paragraph kept exactly, including this punctuation: “source.”',[b['text'] for b in entry['blocks']])
        self.assertNotIn('different indexed',str(entry));self.assertNotIn('Footnotes',str(entry))
        self.assertEqual(self.client.get(self.base+'/api/commentary?book=John&chapter=3&verse=16').json['entries'],[])
        self.assertEqual(self.client.get(self.base+'/api/commentary?book=wrong').status_code,400)
        self.assertEqual(self.client.get(self.base+'/api/commentary?book=Colossians&chapter=5').status_code,400)
        self.assertEqual(path.read_bytes(),before)
        with self.client.session_transaction() as account:account.clear()
        self.assertEqual(self.client.get(self.base+'/api/commentary?book=Colossians').status_code,401)
        absent=Path(self.directory.name)/'absent.db'
        self.assertFalse(self.bible.wiersbe_commentary(absent,51,2,10)['available']);self.assertFalse(absent.exists())

    def test_wiersbe_heading_ranges_include_single_chapter_books_and_disjoint_passages(self):
        parse=self.bible._commentary_ranges
        self.assertEqual([(r['book_id'],r['chapter'],r['verse'],r['end_chapter'],r['end_verse']) for r in parse('Exodus 30:1–10, 17–21; 38:1–20')],
                         [(2,30,1,30,10),(2,30,17,30,21),(2,38,1,38,20)])
        self.assertEqual(parse('1 Kings 8:1—9:9, 25–28')[-1]['chapter'],9)
        self.assertEqual(parse('2 John')[0]['book_id'],63)
        self.assertEqual(parse('Jude 1–7')[0]['end_verse'],7)

        self.assertEqual(parse('1 John 3:11–24')[0]['book_id'],62)
        self.assertEqual(parse('Jeremiah 34—39; 52')[-1]['chapter'],52)
        for invalid in ('Introduction to Isaiah','John 3:50–2:4','John 22','Not a source heading'):
            self.assertEqual(parse(invalid),[])

    def test_moody_uses_book_context_and_paragraph_leading_passages(self):
        doc=dict(id=1,title='The Moody Bible Commentary',author='Source editors')
        first='2:6-7. Walk in Christ with thanksgiving.'
        rows=[(1,'COLOSSIANS\nGERALD PETERMAN\nOUTLINE\nI. Christ (1:1-4:18)'),
              (2,first+'\nAn explanation with an incidental John 3:16 reference that does not change coverage.\n2:8-10. Fullness in Christ.\nOriginal explanation of the following passage, retained without rewriting.'),
              (3,'ROMANS\nANOTHER CONTRIBUTOR\nOUTLINE\nI. Introduction (1:1-17)'),
              (4,'1:1-7. Paul introduces his letter.\nAn original explanation of the letter to Rome, without being added to Colossians.')]
        runs=[dict(text='2:6-7.',marks=['strong']),dict(text=' Walk in Christ with thanksgiving.',marks=[])]
        sections=self.bible._moody_sections(doc,rows,{first:runs})
        match=self.bible._commentary_entries(sections,51,2,7)['entries'][0]
        self.assertEqual(match['passage'],'Colossians 2:6-7')
        self.assertEqual(match['blocks'][0]['runs'],runs)
        self.assertFalse(self.bible._commentary_entries(sections,43,3,16)['entries'])
        self.assertNotIn('Rome',' '.join(block['text'] for block in match['blocks']))

    def test_moody_handles_single_chapter_books_and_shared_kings_outline(self):
        doc=dict(id=1,title='The Moody Bible Commentary',author='Editors')
        rows=[(1,'1 KINGS\nHARRY SHIELDS\nOUTLINE FOR 1 AND 2 KINGS\nIntroduction\n1:1-4. The opening of the first book.\nOriginal commentary on the beginning of the first book of Kings.'),
              (2,'2 KINGS\nHARRY SHIELDS\nIntroduction\n1:1-4. The opening of the second book.\nOriginal commentary on the beginning of the second book of Kings.'),
              (3,'PHILEMON\nGERALD PETERMAN\nOUTLINE\nIntroduction\n1. An introductory numbered point must not become a commentary entry.\nCOMMENTARY ON PHILEMON\n1. Paul introduces his letter to Philemon.\nThis original paragraph explains the opening verse.\n2-3. Paul greets the church.\nThis original paragraph explains the following verses.')]
        sections=self.bible._moody_sections(doc,rows)
        self.assertEqual({r['book_id'] for s in sections for r in s['ranges']},{11,12,57})
        entry=self.bible._commentary_entries(sections,57,1,2)['entries'][0]
        self.assertEqual(entry['passage'],'Philemon 2-3')
        self.assertNotIn('introductory',' '.join(b['text'] for b in entry['blocks']))

    def test_pulpit_lists_only_homiletics_and_homilies_using_their_opening_passage(self):
        doc=dict(id=1,title='The Pulpit Commentary Complete Volume 9',author='Source editors')
        rows=[(1,'EXPOSITION\nColossians 2:1\nVerse explanation.\nNot a sermon section.\nHOMILETICS\nColossians 2:6-7\nWalking in Christ.\nI. ROOTED IN HIM\nA complete original homily with an illustration from John 3:16 that must not change its chapter coverage.\nHOMILIES BY T. CROSKERY\nColossians 2:8-10\nThe fullness of Christ.\nAnother complete original message, with all its supporting explanation and outline retained.\nEXPOSITION\nColossians 3:1\nA commentary paragraph rather than a homily.')]
        source='I. ROOTED IN HIM'
        sections=self.bible._sermon_sections(doc,rows,{source:[dict(text=source,marks=['strong'])]})
        self.assertEqual([s['title'] for s in sections],['Walking in Christ.','The fullness of Christ.'])
        self.assertEqual(sections[0]['ranges'][0]['book_id'],51)
        self.assertEqual(sections[1]['author'],'HOMILIES BY T. CROSKERY')
        self.assertNotIn('A commentary paragraph',sections[1]['text'])
        self.assertTrue(any(b.get('runs') for b in sections[0]['blocks']))

    def test_pulpit_inline_wrapped_and_plain_titles_keep_complete_original_sections(self):
        doc=dict(id=1,title='The Pulpit Commentary Complete Volume 9',author='Editors',format='pdf')
        combined='1Ti. 2:1-15.—Public worship.'
        wrapped='1Ti. 2:3, 1Ti. 2:4.—The beneficial and acceptable nature of such'
        extra='1Ti. 2:5-7.—Prayer and John 3:16.'
        separate='1Ti. 2:8'
        styles={'__source_typography__':True,'__source_heading_citations__':{combined,wrapped,extra,separate}}
        body='A complete original homily paragraph with sufficient exposition, retained exactly as printed in the indexed source.'
        rows=[(1,'1 Timothy 2:1-15\nEXPOSITION\nAn explanation.\nHOMILETICS\n'+combined+'\n'+body+'\nHOMILIES BY T. CROSKERY\n'+wrapped),
              (2,'catholic prayer.\n'+body+'\n'+extra+'\n'+body+'\nJohn 3:16).\n2. A supporting subpoint, not another sermon.\n'+separate+'\nThe conduct of public prayer by men.\n'+body)]
        sections=self.bible._pulpit_sections(doc,rows,styles)
        self.assertEqual([s['title'] for s in sections],['Public worship.','The beneficial and acceptable nature of such catholic prayer.','Prayer and John 3:16.','The conduct of public prayer by men.'])
        self.assertEqual(sections[1]['source_end_section'],2)
        self.assertIn(wrapped+'\ncatholic prayer.',sections[1]['text'])
        self.assertNotIn(extra,sections[1]['text'])
        self.assertEqual({r['book_id'] for s in sections for r in s['ranges']},{54})
        self.assertTrue(all(body in s['text'] for s in sections))
        self.assertIn('CROSKERY',sections[1]['author'])

    def test_pulpit_relative_and_split_reference_headings_use_explicit_chapter_context(self):
        doc=dict(id=1,title='The Pulpit Commentary Complete Volume 9',author='Editors',format='pdf')
        first,second,relative='1Ti. 2:1,','1Ti. 2:2.—Pray for rulers.','Vers. 3-4.—Prayer for all.'
        styles={'__source_typography__':True,'__source_heading_citations__':{first,second,relative}}
        body='An original homily with its complete discussion and practical application, which must stay under this printed heading.'
        rows=[(1,'1 Timothy 2:1-15\nEXPOSITION\nAn explanation.\nHOMILETICS\n'+first),
              (2,second+'\n'+body+'\n'+relative+'\n'+body+'\n1 Timothy 3:1-16\nEXPOSITION\nAn explanation of the next chapter.')]
        sections=self.bible._pulpit_sections(doc,rows,styles)
        self.assertEqual([s['title'] for s in sections],['Pray for rulers.','Prayer for all.'])
        self.assertEqual([r['verse'] for r in sections[0]['ranges']],[1,2])
        self.assertEqual(sections[1]['ranges'][0],dict(book_id=54,chapter=2,verse=3,end_chapter=2,end_verse=4))
        self.assertNotIn('1 Timothy 3',sections[1]['text'])
        self.assertFalse(self.bible._pulpit_sections(doc,[(1,'HOMILETICS\n'+relative+'\n'+body)],styles))

    def test_pulpit_uses_citation_emphasis_without_accepting_body_fragments(self):
        doc=dict(id=1,title='The Pulpit Commentary Complete Volume 9',author='Editors',format='pdf')
        header='1Ti. 2:1.—The regulation of public worship.'
        citation='1Ti. 2:1'
        styles={'__source_typography__':True,header:[dict(text=citation,marks=['strong']),dict(text=header[len(citation):],marks=[])],
                'John 3:16.—A normal body sentence.':[dict(text='John 3:16.—A normal body sentence.',marks=[])]}
        body='Original explanation and application continue here, with an incidental supporting citation rather than another preaching text.'
        rows=[(1,'HOMILETICS\n'+header+'\n'+body+'\nJohn 3:16.—A normal body sentence.\n'+body+'\n1Ti. 99:1.—An invalid citation.\n'+body)]
        sections=self.bible._pulpit_sections(doc,rows,styles)
        self.assertEqual([s['title'] for s in sections],['The regulation of public worship.'])
        self.assertIn('A normal body sentence',sections[0]['text'])

    def test_pulpit_source_aliases_preserve_isaiah_and_resolve_ecclesiastes_in_context(self):
        for citation,book_id in [('Isa. 4:1',23),('I Samuel 4:1',9),('Luk. 2:1',42),('Joh. 3:16',43),('Rth. 1:1',8),('Son. 2:1',22)]:
            self.assertEqual(self.bible.scripture_references(citation)[0]['book_id'],book_id,citation)
        doc=dict(id=1,title='The Pulpit Commentary Complete Volume 4',author='Editors',format='pdf')
        citation='Est. 4:1-3'
        styles={'__source_typography__':True,'__source_heading_citations__':{citation}}
        rows=[(1,'Ecclesiastes 4:1-16\nEXPOSITION\nAn explanation.\nHOMILETICS\n'+citation+'\nTwo pessimistic fallacies.\nThis complete source homily concerns oppression and the living and the dead in Ecclesiastes, with its original wording retained.')]
        section=self.bible._pulpit_sections(doc,rows,styles)[0]
        self.assertEqual(section['ranges'][0]['book_id'],21)
        self.assertEqual(section['blocks'][0]['references'][0]['book_id'],21)
        self.assertTrue(section['text'].startswith(citation))
        self.assertEqual(self.bible._pulpit_citations(citation)[0]['book_id'],17)

    def test_pulpit_source_chapter_can_locate_a_homily_with_a_misprinted_passage(self):
        doc=dict(id=1,title='The Pulpit Commentary Complete Volume 1',author='Editors')
        rows=[(1,'Exodus 36:1-38\nEXPOSITION\nThe construction continues.\nHOMILETICS\nExo. 35:3-7\nSuperabundant giving.\nA complete original homily in the source chapter for Exodus 36, retaining the passage as the edition printed it. An incidental reference cites John 3:16.')]
        sections=self.bible._pulpit_sections(doc,rows,with_blocks=False)
        self.assertEqual(sections[0]['ranges'][0]['chapter'],35)
        with patch.object(self.bible,'sermon_catalogue',return_value=sections):
            self.assertEqual(self.bible.sermons_for_chapter('unused',2,36)[0]['title'],'Superabundant giving.')
            self.assertFalse(self.bible.sermons_for_chapter('unused',43,3))

    def test_pulpit_commentary_separates_exposition_homiletics_and_greek_source_notes(self):
        self.preview(initialize=True)
        path=Path(self.directory.name)/'pulpit-commentary.db'
        def block(text,bold=False):return dict(text=text,runs=[dict(text=text,marks=['strong'] if bold else [])])
        first=[block('1 Timothy 1:1-20',True),block('EXPOSITION',True),block('1Ti. 1:3',True),
               block('Exhorted (παρεκάλεσα). This original explanation means to entreat or beseech.'),
               block('The exact sense of προσμε ῖ ναι is to stay on, or to abide still.')]
        second=[block('This sentence continues the same original verse explanation across a page boundary.'),
                block('1Ti. 1:4',True),block('The next verse has a separate explanation with its original wording.'),
                block('HOMILETICS',True),block('1Ti. 1:3-7',True),block('Faithful teaching.',True),
                block('A full original homiletics section with practical instruction and a complete application to Christian teaching.'),
                block('HOMILIES BY T. CROSKERY',True),block('1Ti. 1:3',True),block('A separate homily.',True),
                block('This original homily belongs in Sermon Outlines and must not leak into the Homiletics commentary section.')]
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute('CREATE TABLE pij_library_documents(id INTEGER,title TEXT,author TEXT,format TEXT,source_type TEXT,searchable INTEGER,checksum TEXT,modified_time TEXT,indexed_at TEXT,chunk_count INTEGER)')
            conn.execute("INSERT INTO pij_library_documents VALUES(1,'The Pulpit Commentary Complete Volume 9','Editors','pdf','public_ebook',1,'hash','now','now',2)")
            conn.execute('CREATE TABLE pij_library_chunks(document_id INTEGER,chunk_number INTEGER,page_start INTEGER,content TEXT)')
            conn.execute('CREATE TABLE pij_library_source_formats(document_id INTEGER,checksum TEXT,modified_time TEXT,version INTEGER,sections_json TEXT)')
            conn.execute('INSERT INTO pij_library_source_formats VALUES(1,?,?,1,?)',('hash','now',json.dumps([first,second])))
        self.app.config['AI_DATABASE']=str(path)
        query=self.base+'/api/commentary?series=pulpit&book=1+Timothy&chapter=1&verse=3'
        data=self.client.get(query).json
        self.assertTrue(data['available']);self.assertEqual(len(data['entries']),1)
        entry=data['entries'][0];text=' '.join(b['text'] for b in entry['blocks'])
        self.assertIn('continues the same',text);self.assertNotIn('next verse',text);self.assertNotIn('homiletics section',text)
        greek=next(b for b in entry['blocks'] if b.get('greek_links'))
        link=greek['greek_links'][0];self.assertEqual(greek['text'][link['start']:link['end']],'παρεκάλεσα')
        self.assertIn('_study_texts',greek,'Pulpit source selection must retain signed reading anchors')
        self.assertTrue(all('runs' not in anchor['path'] for anchor in data['study_anchors']))
        self.assertTrue(all('_study_texts' not in run for block in entry['blocks'] for run in block.get('runs',[])))
        hom=self.client.get(query+'&section=homiletics').json
        self.assertEqual([e['title'] for e in hom['entries']],['Faithful teaching.'])
        self.assertNotIn('separate homily',' '.join(b['text'] for b in hom['entries'][0]['blocks']))
        self.assertEqual(self.client.get(query+'&section=invalid').status_code,400)
        self.assertFalse(self.client.get(query.replace('verse=3','verse=5')).json['entries'])
        self.assertIsNone(self.client.get(self.base+'/api/pulpit-greek?word=παρεκάλεσα&book=1+Timothy&chapter=1&verse=3').json['match'])

    def test_pulpit_exposition_keeps_chapter_introductions_and_resolves_relative_headings(self):
        doc=dict(id=1,title='The Pulpit Commentary Complete Volume 9',author='Editors')
        rows=[(1,'1 Timothy 2:1-15\nEXPOSITION\nA complete original introduction to this chapter.\nVer. 1.\nAn original explanation of the first verse.\nVers. 2–3\nAn original explanation of the next two verses.\nHOMILETICS\n1Ti. 2:1\nA homiletics title.\nThe homiletics content stays separate.')]
        sections=self.bible._pulpit_exposition_sections(doc,rows)
        self.assertEqual([s['ranges'][0]['verse'] for s in sections],[1,1,2])
        self.assertEqual(sections[-1]['ranges'][0]['end_verse'],3)
        self.assertTrue(all('homiletics content' not in s['text'] for s in sections))

    def test_pulpit_greek_dictionary_matches_only_unambiguous_original_passage_spelling(self):
        self.preview(initialize=True);self.publish_dictionaries();self.import_step_fixture()
        with closing(sqlite3.connect(self.path)) as conn:
            conn.row_factory=sqlite3.Row
            spelling=conn.execute("SELECT spelling FROM bible_original_tokens WHERE book_id=43 AND chapter=1 AND verse=1 AND language='Greek' LIMIT 1").fetchone()[0]
            result=self.bible.pulpit_greek_dictionary_match(conn,' '.join(spelling),43,1,1)
            self.assertIsNotNone(result)
            self.assertIsNone(self.bible.pulpit_greek_dictionary_match(conn,spelling,43,4,16))
            self.assertIsNone(self.bible.pulpit_greek_dictionary_match(conn,'ανύπαρκτη',43,3,16))
            self.assertFalse(conn.execute('SELECT 1 FROM bible_niv_alignments').fetchone())

    def test_pulpit_first_timothy_api_returns_chapter_titles_instead_of_filtering_by_verse(self):
        self.preview(initialize=True)
        path=Path(self.directory.name)/'pulpit.db'
        source=[]
        for chapter in range(1,7):
            lines=[f'1 Timothy {chapter}:1-15','EXPOSITION','Verse explanation.','HOMILETICS',
                   f'1Ti. {chapter}:1.—A printed chapter {chapter} theme.',
                   'A complete original homily for this chapter, with its explanation and practical application retained. A supporting illustration cites John 3:16.']
            source.append([dict(text=line,runs=[dict(text=line,marks=['strong'] if i in (0,3,4) else [])]) for i,line in enumerate(lines)])
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute('CREATE TABLE pij_library_documents(id INTEGER,title TEXT,author TEXT,format TEXT,source_type TEXT,searchable INTEGER,checksum TEXT,modified_time TEXT,indexed_at TEXT,chunk_count INTEGER)')
            conn.execute("INSERT INTO pij_library_documents VALUES(1,'The Pulpit Commentary Complete Volume 9','Editors','pdf','public_ebook',1,'hash','now','now',6)")
            conn.execute('CREATE TABLE pij_library_chunks(document_id INTEGER,chunk_number INTEGER,page_start INTEGER,content TEXT)')
            conn.execute('CREATE TABLE pij_library_source_formats(document_id INTEGER,checksum TEXT,modified_time TEXT,version INTEGER,sections_json TEXT)')
            conn.execute('INSERT INTO pij_library_source_formats VALUES(1,?,?,1,?)',('hash','now',json.dumps(source)))
        self.app.config['AI_DATABASE']=str(path)
        for chapter in range(1,7):
            data=self.client.get(self.base+f'/api/sermons?book=1+Timothy&chapter={chapter}').json
            self.assertEqual([s['title'] for s in data['sermons']],[f'A printed chapter {chapter} theme.'])
        first=self.client.get(self.base+'/api/sermons?book=1+Timothy&chapter=2&verse=1').json
        last=self.client.get(self.base+'/api/sermons?book=1+Timothy&chapter=2&verse=15').json
        self.assertEqual(first,last)
        self.assertEqual(set(first['sermons'][0]),{'id','title'})
        article=self.client.get(self.base+'/api/sermons/'+first['sermons'][0]['id']).json['sermon']
        self.assertTrue(any('complete original homily' in block['text'] for block in article['blocks']))
        self.assertTrue(any(block.get('runs') for block in article['blocks']))

    def test_apeo_mixed_numbered_and_unnumbered_themes_use_foundational_text(self):
        doc=dict(id=1,title='Sermon Resources Vol 1',author='William H. Robertson')
        rows=[(1,'1. JOY IN FELLOWSHIP\nPhilippians 1:1-7\n(KJV)\nPhil 1:1-7\nA complete original introduction and explanation.\nI. FELLOWSHIP\nAn illustration references John 3:16, which is not the main preaching text.'),
              (2,'Notes\nJOY IN PRAYER\nPhilippians 1:8-11\n(KJV)\nA complete original sermon opening and exposition with enough content to be recognized.\nI. PRAYER\nFurther original outline content.')]
        sections=self.bible._sermon_sections(doc,rows)
        self.assertEqual([s['title'] for s in sections],['JOY IN FELLOWSHIP','JOY IN PRAYER'])
        self.assertEqual(sections[1]['ranges'][0]['verse'],8)
        self.assertTrue(any(b.get('role')=='commentary_reference' for b in sections[0]['blocks']))
        self.assertFalse(any(block['text']=='Notes' for section in sections for block in section['blocks']))
        self.assertFalse(any(block['text']=='1. JOY IN FELLOWSHIP' for block in sections[0]['blocks']))

    def test_apeo_wrapped_theme_stays_one_complete_sermon(self):
        doc=dict(id=1,title='Sermon Resources Vol 1',author='Printed author')
        rows=[(1,'1. A LIFE OF\nJOYFUL FELLOWSHIP\nPhilippians 1:1-7\n(KJV)\nThis complete original sermon opening and explanation remains under its full printed theme.\nI. FELLOWSHIP\nAll the following material belongs to this one message.')]
        sections=self.bible._sermon_sections(doc,rows)
        self.assertEqual([s['title'] for s in sections],['A LIFE OF JOYFUL FELLOWSHIP'])
        self.assertEqual(sections[0]['ranges'][0]['book_id'],50)

    def test_sermon_catalogue_matches_chapters_not_incidental_verses_and_keeps_nicoll_last(self):
        self.preview(initialize=True)
        path=Path(self.directory.name)/'sermons.db'
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute('''CREATE TABLE pij_library_documents(id INTEGER,title TEXT,author TEXT,format TEXT,
                source_type TEXT,searchable INTEGER,checksum TEXT,modified_time TEXT,indexed_at TEXT,chunk_count INTEGER)''')
            conn.execute('CREATE TABLE pij_library_chunks(document_id INTEGER,chunk_number INTEGER,page_start INTEGER,content TEXT)')
            docs=[(1,'Sermon Resources Vol 1','pdf','public_ebook'),(2,'Sermon Resources Vol 1-unlocked','pdf','public_ebook'),
                  (3,'The-Shepherds-Staff','pdf','public_ebook'),(4,'The Sermon Outline Bible Commentary','pdf','public_ebook'),
                  (5,'52 Funeral Sermons','epub','public_ebook'),(6,'Sermon Resources Vol 8','pdf','private_sermon')]
            for did,title,fmt,scope in docs:
                conn.execute('INSERT INTO pij_library_documents VALUES(?,?,?, ?,?,1,?,?,?,2)',(did,title,'Unknown Author',fmt,scope,'hash','now','now'))
            shared='1. ZEALOUS HOPE\nColossians 2:20–3:2\nAn original outline opening that states its preaching passage explicitly and retains its original wording.\nI. EXPOSITION\nAn incidental illustration cites John 3:16; it does not change this sermon’s preaching text.'
            for did in (1,2,6):
                conn.execute('INSERT INTO pij_library_chunks VALUES(?,1,1,?)',(did,'By\nWilliam H. Robertson'))
                conn.execute('INSERT INTO pij_library_chunks VALUES(?,2,2,?)',(did,shared))
            conn.execute('INSERT INTO pij_library_chunks VALUES(3,1,1,?)',('Chapter 1\nTeaching\nA. NEW LIFE\nJohn 3:1-9\nAn original opening for a teaching section with clearly identified scripture.\n1. EXPLANATION\nThis later illustration mentions Colossians 2:1 and must not become main passage coverage.',))
            conn.execute('INSERT INTO pij_library_chunks VALUES(4,1,1,?)',('Chapter 2\nColossians 2:1-2\nI. An original sermon outline paragraph long enough to identify an actual article rather than a table of contents or a bibliographic reference.',))
            conn.execute('INSERT INTO pij_library_chunks VALUES(5,1,1,?)',('Table of Contents\n1. COMFORT FOR THE BEREAVED',))
            conn.execute('INSERT INTO pij_library_chunks VALUES(5,2,2,?)',('1. COMFORT FOR THE BEREAVED\nRevelation 14:13\nAn original funeral outline whose purpose and scripture are identified in its source, with complete contents retained here.',))
        self.app.config['AI_DATABASE']=str(path);before=path.read_bytes()
        first=self.client.get(self.base+'/api/sermons?book=Colossians&chapter=2&verse=1').json
        second=self.client.get(self.base+'/api/sermons?book=Colossians&chapter=2&verse=23').json
        self.assertEqual(first,second)
        self.assertEqual([item['title'] for item in first['sermons']],['ZEALOUS HOPE','Colossians 2:1-2'])
        self.assertEqual(set(first['sermons'][0]),{'id','title'})
        detail=self.client.get(self.base+'/api/sermons/'+first['sermons'][0]['id']).json['sermon']
        self.assertEqual(detail['author'],'William H. Robertson')
        self.assertIn('John 3:16','\n'.join(block['text'] for block in detail['blocks']))
        self.assertEqual([item['title'] for item in self.client.get(self.base+'/api/sermons?book=Colossians&chapter=3').json['sermons']],['ZEALOUS HOPE'])
        john=self.client.get(self.base+'/api/sermons?book=John&chapter=3').json['sermons']
        self.assertEqual([item['title'] for item in john],['Teaching'])
        staff=self.client.get(self.base+'/api/sermons/'+john[0]['id']).json['sermon']
        self.assertIn('teaching',staff['purpose']);self.assertIn('editor',staff['author'])
        funeral=self.client.get(self.base+'/api/sermons?book=Revelation&chapter=14').json['sermons']
        self.assertEqual(funeral[0]['title'],'COMFORT FOR THE BEREAVED')
        self.assertEqual(self.client.get(self.base+'/api/sermons?book=bad&chapter=2').status_code,400)
        self.assertEqual(path.read_bytes(),before)
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute("UPDATE pij_library_documents SET checksum='changed' WHERE id=1")
            conn.execute("UPDATE pij_library_chunks SET content=replace(content,'ZEALOUS HOPE','CHANGED HOPE') WHERE document_id=1")
        self.assertEqual(self.client.get(self.base+'/api/sermons?book=Colossians&chapter=2').json['sermons'][0]['title'],'CHANGED HOPE')
        with self.client.session_transaction() as account:account.clear()
        self.assertEqual(self.client.get(self.base+'/api/sermons?book=Colossians&chapter=2').status_code,401)

    def test_study_saves_validate_signed_unicode_sources_preserve_order_and_are_account_scoped(self):
        headers=self.preview(initialize=True)
        content='Alpha 😀 beta.'
        provider=dict(available=True,entries=[dict(title='A source article',source_title='Wiersbe fixture',author='Fixture',passage='Colossians 2:1-23',
            blocks=[dict(type='paragraph',text=content),dict(type='paragraph',text='Second original source paragraph.')])])
        with patch.object(self.bible,'bible_study_be_commentary',return_value=provider):
            data=self.client.get(self.base+'/api/commentary?book=Colossians&chapter=2&verse=1').json
            other=self.client.get(self.base+'/api/commentary?book=Colossians&chapter=2&verse=23').json
        anchor=next(anchor for anchor in data['study_anchors'] if anchor['text']==content)
        self.assertEqual(anchor['key'],next(a for a in other['study_anchors'] if a['text']==content)['key'])
        span=dict(token=anchor['token'],text=content,start_offset=6,end_offset=12,selected_text='😀 beta')
        payload=dict(kind='highlight',color='#9ee6b8',note='',ranges=[span])
        self.assertEqual(self.client.post(self.base+'/api/study-saves',json=payload).status_code,400)
        for changes in ({'token':anchor['token']+'forged'},{'text':'Changed source'},{'start_offset':7}):
            invalid={**payload,'ranges':[{**span,**changes}]}
            self.assertEqual(self.client.post(self.base+'/api/study-saves',json=invalid,headers=headers).status_code,400)
        result=self.client.post(self.base+'/api/study-saves',json=payload,headers=headers)
        self.assertEqual(result.status_code,201,result.json)
        item=result.json['item'];self.assertIn('study=',item['jump_url'])
        self.assertEqual(item['ranges'][0]['selected_text'],'😀 beta')
        url=item['delete_url']
        self.assertEqual(self.client.patch(url,json=dict(note='My source note',version=1),headers=headers).status_code,200)
        self.assertEqual(self.client.patch(url,json=dict(note='Stale',version=1),headers=headers).status_code,409)
        bookmark={**payload,'kind':'bookmark'}
        saved=self.client.post(self.base+'/api/study-saves',json=bookmark,headers=headers).json['item']
        again=self.client.post(self.base+'/api/study-saves',json=bookmark,headers=headers).json['item']
        self.assertEqual(saved['id'],again['id'])
        with self.app.test_request_context():
            library=self.bible.bible_personal_library(self.app,'area overseer:pijeme')
            self.assertEqual(library['annotations'][0]['format'],'STUDY')
        self.login('Other',pastor=True);self.client.get(self.base)
        with self.client.session_transaction() as account:other_headers={'X-CSRF-Token':account['bible_study_csrf']}
        self.assertEqual(self.client.get(url).status_code,404)

        self.assertEqual(self.client.delete(url,json={},headers=other_headers).status_code,404)
        self.assertEqual(self.client.get(self.base+'/api/personal').json['study_annotations'],[])
        self.login();headers=self.preview()
        self.assertEqual(self.client.get(url).json['item']['ranges'][0]['block_text'],content)
        self.assertEqual(self.client.delete(url,json={},headers=headers).status_code,200)
        self.assertEqual(self.client.get(url).status_code,404)


    def test_study_save_upgrade_is_explicit_additive_and_creates_one_recovery_backup(self):
        headers=self.preview(initialize=True);self.publish_fixture()
        verse=self.client.get(self.base+'/api/chapter').json['verses'][0]
        saved=self.client.post(self.base+'/api/bookmarks',json=dict(verse_id=verse['id']),headers=headers).json['bookmark']
        with closing(sqlite3.connect(self.path)) as conn,conn:
            conn.execute('DROP TABLE bible_study_saves')
        self.assertEqual(self.client.get(self.base+'/api/personal').json['study_annotations'],[])
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE name='bible_study_saves'").fetchone())
        report=self.bible.initialize_study_saves(self.path)
        self.assertEqual(report['status'],'completed');backup=Path(report['backup']);self.assertTrue(backup.is_file())
        with closing(sqlite3.connect(backup)) as conn:
            self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE name='bible_study_saves'").fetchone())
            self.assertEqual(conn.execute('SELECT id FROM bible_bookmarks').fetchone()[0],saved['id'])
        self.assertEqual(self.client.get(saved['delete_url']).status_code,200)
        self.assertEqual(self.bible.initialize_study_saves(self.path)['status'],'unchanged')
        self.assertEqual(len(list(backup.parent.glob('*.db'))),1)
    def test_exalting_outline_formats_body_hierarchy_without_changing_source_text_or_links(self):
        lines=['Main Idea: A source introduction.',
               'I. Our Life in Christ (2:6-7).','A. Walk in him (2:6).','1. Keep trusting (2:6).',
               'II. A duplicated printed title (21-22).','A. Trust his fullness (2:8).',
               'II. A duplicated printed title (21-22).','A. Receive new life (2:11).',
               'This is ordinary explanatory prose that must remain a paragraph.',
               'Our Life in Christ','Colossians 2:6-7','Walk in Him ( 2:6)','Colossians 2:6','Keep Trusting (2:6)',
               'The Fullness of Christ','Colossians 2:8-10','Trust His Fullness (2:8)',
               'Our Identity in Christ','Colossians 2:11-15','Receive New Life (2:11)',
               'Conclusion','This closing sentence remains ordinary prose.']
        section=dict(document_id=1,source_title='Exalting Jesus fixture',author='Source author',title='Source section',
            passage='Colossians 2:6-15',source_section=7,ranges=self.bible._commentary_ranges('Colossians 2:6-15'),text='\n'.join(lines))
        result=self.bible._commentary_entries([section],51,2,8,format_outline=True)['entries'][0]['blocks']
        self.assertEqual([b['text'] for b in result],lines)
        headings=[b for b in result if b.get('role')=='commentary_heading']
        self.assertEqual([(b['marker'],b['text']) for b in headings if b['level']==1],
                         [('I.','Our Life in Christ'),('II.','The Fullness of Christ'),('III.','Our Identity in Christ')])
        self.assertEqual(next(b['level'] for b in headings if b['text']=='Keep Trusting (2:6)'),3)
        self.assertEqual(next(b['level'] for b in result if b['text']=='A. Walk in him (2:6).'),2)
        self.assertNotIn('role',result[8]);self.assertNotIn('role',result[-1])
        for block in result:
            for ref in block.get('references',[]):
                self.assertEqual(block['text'][ref['start']:ref['end']],ref['source_text'])
        plain=self.bible._commentary_entries([section],51,2,8)['entries'][0]['blocks']
        self.assertFalse(any('marker' in b or 'role' in b for b in plain),'Wiersbe formatting must remain unchanged')
        section['text']=section['text'].replace('\nOur Life in Christ\n','\nI. Our Life in Christ\n')
        existing=self.bible._commentary_entries([section],51,2,8,format_outline=True)['entries'][0]['blocks']
        heading=next(b for b in existing if b['text']=='I. Our Life in Christ')
        self.assertEqual(heading['role'],'commentary_heading');self.assertIsNone(heading['marker'],'Do not duplicate a printed body marker')

    def test_exalting_commentary_reads_epub_pdf_and_epigraph_ranges_without_changing_index(self):
        import json
        self.preview(initialize=True)
        path=Path(self.directory.name)/'exalting-index.db'
        with closing(sqlite3.connect(path)) as conn,conn:
            conn.execute('''CREATE TABLE pij_library_documents(id INTEGER,title TEXT,author TEXT,format TEXT,
                source_type TEXT,searchable INTEGER,checksum TEXT,modified_time TEXT,indexed_at TEXT,chunk_count INTEGER)''')
            conn.execute('CREATE TABLE pij_library_chunks(document_id INTEGER,chunk_number INTEGER,page_start INTEGER,content TEXT)')
            for did,title,fmt in ((1,'Exalting Jesus in Colossians','epub'),(2,'Exalting Jesus in Colossians','pdf'),
                                  (3,'Exalting Jesus in Psalms 1-50','pdf'),(4,'Exalting Jesus in Isaiah','epub')):
                conn.execute("INSERT INTO pij_library_documents VALUES(?,?,'Commentaries',?,'public_ebook',1,'hash','now','now',3)",(did,title,fmt))
            chunks={1:[(1,1,'Copyright 2021\n© Copyright 2021 by Source Author\nContents\nA fake contents heading\nColossians 2:1-15\nOutline entry without an article.'),
                       (2,7,'Exalting Jesus in Colossians\nGrace for the Family\nCOLOSSIANS 1:1-8\nMain Idea: This is the first article, covering the first eight verses.\nAn incidental citation to John 3:16 is not this article’s passage.\nWalking in Christ (Colossians 2:6-15)\nMain Idea: This original article explains the printed passage.\nOriginal paragraph one, preserved exactly.\n'),
                       (3,8,'Exalting Jesus in Colossians\nOriginal continuation paragraph, including “unchanged” wording.\nWorks Cited\nBack matter must not appear in the commentary.')],
                    2:[(1,1,'Duplicate PDF Heading\nColossians 2:6-15\nMain Idea: A duplicate version must not appear alongside the preferred EPUB.')],
                    3:[(1,20,'5\nOur Shepherd\nand His Care\nPSALM 23\nMain Idea: The original PDF article explains the shepherd psalm.\nOriginal PDF paragraph.'),
                       (2,21,'6\nPDF continuation paragraph kept exactly.\n'),
                       (3,22,'7\nThe King of Glory\nPSALM 24\nMain Idea: The next PDF article begins here and must not leak into Psalm 23.\nA different source paragraph.')],
                    4:[(1,10,'Exalting Jesus in Isaiah\nThe Father and His Children\nIsaiah 1\nA source epigraph quotation (Isaiah 1:18-20)\nMain Idea: The article covers Isaiah chapter one, including verses outside its epigraph.\nAn exact original paragraph about Isaiah 1.')]
                   }
            for did,items in chunks.items():
                for ordinal,page,text in items:conn.execute('INSERT INTO pij_library_chunks VALUES(?,?,?,?)',(did,ordinal,page,text))
        self.app.config['AI_DATABASE']=str(path);before=path.read_bytes()
        data=self.client.get(self.base+'/api/commentary?series=exalting-jesus&book=Colossians&chapter=2&verse=10').json
        self.assertEqual(data['series'],'exalting-jesus');self.assertEqual(len(data['entries']),1)
        entry=data['entries'][0]
        self.assertEqual((entry['title'],entry['passage'],entry['author']),('Walking in Christ','Colossians 2:6-15','Source Author'))
        self.assertIn('Original continuation paragraph, including “unchanged” wording.',[b['text'] for b in entry['blocks']])
        self.assertNotIn('Back matter',str(entry));self.assertNotIn('Duplicate PDF',str(entry))
        pdf=self.client.get(self.base+'/api/commentary?series=exalting-jesus&book=Psalms&chapter=23&verse=1').json['entries'][0]
        self.assertEqual(pdf['title'],'Our Shepherd and His Care')
        self.assertIn('PDF continuation paragraph kept exactly.',[b['text'] for b in pdf['blocks']]);self.assertNotIn('next PDF article',str(pdf))
        epigraph=self.client.get(self.base+'/api/commentary?series=exalting-jesus&book=Isaiah&chapter=1&verse=1').json['entries'][0]
        self.assertEqual((epigraph['title'],epigraph['passage']),('The Father and His Children','Isaiah 1'))
        self.assertEqual(epigraph['blocks'][0]['text'],'A source epigraph quotation (Isaiah 1:18-20)')
        self.assertEqual(self.client.get(self.base+'/api/commentary?series=exalting-jesus&book=John&chapter=3&verse=16').json['entries'],[])
        self.assertEqual(self.client.get(self.base+'/api/commentary?series=unsupported&book=John').status_code,400)
        self.assertEqual(self.client.get(self.base+'/api/commentary?book=Colossians').json['series'],'wiersbe')
        self.assertEqual(path.read_bytes(),before)
        with self.client.session_transaction() as account:account.clear()
        self.assertEqual(self.client.get(self.base+'/api/commentary?series=exalting-jesus&book=John').status_code,401)

    def test_major_chain_rows_link_only_printed_dotted_leaders(self):
        import json
        material,edition=self.thompson_study_fixture()
        with closing(sqlite3.connect(self.path)) as conn,conn:
            conn.row_factory=sqlite3.Row
            conn.execute('''INSERT INTO bible_chain_topics(edition_id,topic_key,title,content_json,source_page)
                VALUES(?,'1692','Eternal Hope','[]',1600)''',(edition,))
            blocks=[dict(type='paragraph',text='Ordinary prose 1692. Genesis 1:1.'),
                    dict(type='heading',text='MAJOR CHAINS IN GENESIS'),
                    dict(type='paragraph',text='Eternal Hope. . . . . 1692 Colossae. . . . . 4358 Joshua. . . . . 1922, 4290')]
            content=dict(blocks=blocks,outline=dict(blocks=[dict(type='paragraph',text='Exact source outline.')],source_page=1888))
            conn.execute("UPDATE bible_thompson_studies SET content_json=? WHERE study_key='book:1'",(json.dumps(content),))
        data=self.client.get(self.base+'/api/book-study?book=Genesis').json['study']
        self.assertEqual({k:v for k,v in data['blocks'][0].items() if k!='_study_texts'},blocks[0])
        rows=data['blocks'][2:];self.assertEqual([r['text'] for r in rows],['Eternal Hope · 1692','Colossae · 4358','Joshua · 1922','Joshua · 4290'])
        self.assertTrue(rows[0]['chain_links'][0]['topic_id']);self.assertIsNone(rows[1]['chain_links'][0]['topic_id'])
        self.assertEqual(data['outline']['blocks'][0]['text'],'Exact source outline.')
        missing=self.client.get(self.base+'/api/thompson-reference/4358').json['topic']
        self.assertIn('not included',missing['blocks'][0]['text'])
        self.assertEqual(self.client.get(self.base+'/api/thompson-reference/bad').status_code,400)
        outline=self.client.get(self.base+'/api/thompson-reference/4220').json['topic']
        self.assertEqual(outline['blocks'][0]['text'],'Exact source outline.')
        self.assertEqual(outline['source_page'],1888)

    def test_maxwell_cache_generates_once_and_missing_notes_stay_invisible(self):
        import time
        self.preview(initialize=True)
        index=self.maxwell_index_fixture()
        with patch.object(self.bible,'find_maxwell_note',wraps=self.bible.find_maxwell_note) as generate:
            self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21')
            deadline=time.monotonic()+5
            result,pending=None,True
            while pending and time.monotonic()<deadline:
                time.sleep(.01)
                result,pending=self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21')
            self.assertEqual(result['excerpt'],result['content'])
            self.assertNotIn('summary',result)
            self.assertEqual(result['content'],'Leaders bear responsibility and help others with their burdens.')
            self.assertNotIn('expanded',result)
            self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21')
            self.assertEqual(generate.call_count,1)
            self.bible.daily_maxwell_insight(self.path,index,'John 3:16','Synthetic verse.','2026-10-21')
            deadline=time.monotonic()+5
            while time.monotonic()<deadline:
                time.sleep(.01)
                result,pending=self.bible.daily_maxwell_insight(self.path,index,'John 3:16','Synthetic verse.','2026-10-21')
                if not pending:break
            self.assertEqual((result,pending),(None,False))
            self.assertEqual(generate.call_count,2)
        from flask import render_template
        with self.app.test_request_context():
            html=render_template('bulletin.html',verse_reference='John 3:16',verse_text='Synthetic verse.',leadership_insight=None,
                                 insight_pending=False,today_str='Today',area_number='1',posts=[],current_church='')
        self.assertNotIn('maxwellInsight',html)
        self.assertNotIn('Leadership Insight',html)

    def test_dictionary_sync_rollback_and_unchanged_fingerprints_preserve_records(self):
        self.preview(initialize=True)
        self.publish_fixture()
        selected,parsed,_=self.publish_dictionaries()
        tables=['bible_verses','bible_tokens','bible_concordance','bible_lexicon','bible_dictionary_entries','bible_dictionary_versions']
        with closing(sqlite3.connect(self.path)) as conn:
            before=[conn.execute('SELECT COUNT(*) FROM '+table).fetchone()[0] for table in tables]
        invalid_sources=copy.deepcopy(selected)
        invalid_sources['mounce']['sha256']='e'*64
        invalid_sources['vine']['sha256']='f'*64
        invalid_parsed=copy.deepcopy(parsed)
        invalid_parsed['vine']['entries'][0]['identifiers'].append(('strong','Hebrew',0,'','invalid identifier'))
        with self.assertRaises(sqlite3.IntegrityError):
            self.bible._publish_dictionary_sync(self.path,invalid_sources,invalid_parsed,'fixture_folder_id',[])
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual([conn.execute('SELECT COUNT(*) FROM '+table).fetchone()[0] for table in tables],before)
        with patch.object(self.bible,'_official_drive_sources',return_value=('fixture_folder_id',selected,[])), \
             patch.object(self.bible,'_download_source',side_effect=AssertionError('Unchanged source downloaded')), \
             patch.object(self.bible,'_library_sources',side_effect=AssertionError('Library dependency')):
            report=self.bible.sync_sources(self.path,'fixture_folder_id',lambda _:None,drive=Mock())
        self.assertIn('unchanged',report['message'])

    def test_maxwell_pending_lease_prevents_duplicate_source_reads(self):
        from threading import Event
        import time
        self.preview(initialize=True)
        index=self.maxwell_index_fixture()
        started,release=Event(),Event()
        def generate(*_):
            started.set()
            release.wait(timeout=5)
            return dict(body='Exact indexed fixture content.',title='Fixture',passage='Galatians 6:1-6')
        before=index.read_bytes()
        with patch.object(self.bible,'find_maxwell_note',side_effect=generate) as model:
            try:
                self.assertEqual(self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21'),(None,True))
                self.assertTrue(started.wait(timeout=5))
                for _ in range(4):
                    self.assertEqual(self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21'),(None,True))
                self.assertEqual(model.call_count,1)
            finally:
                release.set()
                deadline=time.monotonic()+5
                while time.monotonic()<deadline:
                    time.sleep(.01)
                    insight,pending=self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21')
                    if not pending:break
                self.assertTrue(insight)
        self.assertEqual(index.read_bytes(),before)

    def test_maxwell_devotional_uses_original_text_without_calling_ai(self):
        import sys,time
        self.preview(initialize=True)
        index=self.maxwell_index_fixture()
        fake=SimpleNamespace(ask_model=Mock(side_effect=AssertionError('Devotional called AI')))
        with patch.dict(sys.modules,{'ai_assistant':fake}):
            result,pending=self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21')
            deadline=time.monotonic()+5
            while pending and time.monotonic()<deadline:
                time.sleep(.01)
                result,pending=self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21')
            self.assertTrue(result['content'].startswith(result['excerpt']))
            self.assertNotIn('summary',result)
            fake.ask_model.assert_not_called()

    def test_bulletin_insight_endpoint_requires_login_and_uses_only_daily_reference(self):
        import ast
        from flask import jsonify,session
        from datetime import datetime
        from zoneinfo import ZoneInfo
        tree=ast.parse((Path(resources.__file__).parent/'app.py').read_text(encoding='utf-8-sig'))
        function=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='bulletin_leadership_insight')
        daily=Mock(return_value=(None,False))
        namespace=dict(app=self.app,jsonify=jsonify,datetime=datetime,PH_TZ=ZoneInfo('Asia/Manila'),
                       AI_DATABASE=str(Path(self.directory.name)/'ai_index.db'),get_verse_of_the_day=Mock(return_value=('Genesis 1:1','Synthetic verse.')),
                       daily_maxwell_insight=daily,any_user_logged_in=lambda:session.get('pastor_logged_in') is True or session.get('ao_logged_in') is True)
        exec(compile(ast.Module(body=[function],type_ignores=[]),'app.py','exec'),namespace)
        self.assertEqual(self.client.get('/bulletin/leadership-insight').status_code,401)
        daily.assert_not_called()
        self.login('Pastor',pastor=True)
        response=self.client.get('/bulletin/leadership-insight?reference=John3:16')
        self.assertEqual(response.status_code,200)
        self.assertIsNone(response.json['insight'])
        self.assertEqual(daily.call_args.args[2:4],('Genesis 1:1','Synthetic verse.'))
        self.assertEqual(response.headers['Cache-Control'],'private, no-store')

    def test_bulletin_renders_cached_summary_with_source_and_escapes_ai_text(self):
        from flask import render_template
        self.app.add_url_rule('/bulletin/leadership-insight',endpoint='bulletin_leadership_insight',view_func=lambda:'')
        with self.app.test_request_context():
            html=render_template('bulletin.html',verse_reference='Genesis 1:1',verse_text='Synthetic verse.',
                leadership_insight=dict(title='<script>untrusted</script>',passage='Genesis 1:1-2',summary='Fixture summary.',content='Exact indexed article.\nSecond paragraph <source>.'),
                insight_pending=False,verse_day='2026-10-21',today_str='Today',area_number='1',posts=[],current_church='')
        self.assertIn('Maxwell Leadership Bible',html)
        self.assertNotIn('AI-generated summary',html)
        self.assertIn('class="maxwell-insight"',html)
        self.assertIn('Exact indexed article.\nSecond paragraph &lt;source&gt;.',html)
        self.assertIn('Genesis 1:1-2',html)
        self.assertIn('&lt;script&gt;untrusted&lt;/script&gt;',html)
        self.assertNotIn('<script>untrusted</script>',html)


if __name__ == '__main__':
    unittest.main()
