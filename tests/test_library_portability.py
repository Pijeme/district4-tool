"""Portability and incremental sync checks use temporary databases and fake Drive."""

import copy
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
            self.assertEqual(db.execute("SELECT is_active FROM pastor_library_files WHERE drive_file_id='removed'").fetchone()[0], 0)
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
        self.app.config.update(TESTING=True, BIBLE_STUDY_DATABASE=str(self.path))
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

    def fixture_epub(self, documents, assets=None):
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
            archive.writestr('OEBPS/content.opf', '<package><manifest>' + ''.join(manifest) + '</manifest><spine>' + ''.join(spine) + '</spine></package>')
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
            self.assertEqual(match['sources'], [])
            self.assertEqual(match['message'], self.bible.NO_VERIFIED_ALIGNMENT)
            self.assertNotIn('KJV', str(match))
            self.assertEqual(self.client.get(f'{self.base}/api/concordance/1').status_code, 404)
            preview = self.client.get(self.base + '/api/verse?book=Genesis&chapter=1&verse=1').json['verse']
            self.assertEqual(preview['text'], verse['text'])
            self.assertEqual(preview['reference'], 'Genesis 1:1')
        self.assertEqual(self.client.get(self.base + '/api/verse?book=Genesis&chapter=1&verse=2').status_code, 404)
        self.assertEqual(self.client.get(self.base + '/api/chapter?book=Jude&chapter=2').status_code, 400)
        self.assertEqual(self.client.get(self.base + '/api/status').json['indexed_verses'], 1)

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
        def blocked_sync(path, library_path, progress):
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

    def test_dictionary_search_is_source_specific_and_assets_require_login(self):
        self.preview(initialize=True)
        self.publish_fixture()
        self.publish_dictionaries()
        results = self.client.get(self.base + '/api/dictionaries/search?q=alpha').json
        self.assertEqual([source['label'] for source in results['sources']], ['Mounce','Vine'])
        self.assertEqual([len(source['entries']) for source in results['sources']], [3,2])
        headword = results['sources'][0]['entries'][0]
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
            self.assertTrue(response.json['sources'][0]['entries'])
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
        with patch.object(self.bible,'search_dictionaries',side_effect=AssertionError('No English fallback')):
            result=self.client.get(url).json
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
        self.assertEqual([g['label'] for g in result['sources']],['Mounce','Vine'])
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
        noah=self.client.get(self.base+'/api/dictionaries/search?q=Noah').json['people']['selected']
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
            self.assertEqual(result['blocks'],blocks)
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
        data=self.client.get(self.base+'/api/commentary?book=Colossians&chapter=2&verse=10').json
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
        self.assertEqual(data['blocks'][0],blocks[0])
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
        summary=dict(summary='Synthetic supported summary.',expanded='Synthetic expanded explanation.')
        with patch.object(self.bible,'_summarize_maxwell_note',return_value=summary) as generate:
            self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21')
            deadline=time.monotonic()+5
            result,pending=None,True
            while pending and time.monotonic()<deadline:
                time.sleep(.01)
                result,pending=self.bible.daily_maxwell_insight(self.path,index,'Galatians 6:2','Synthetic verse.','2026-10-21')
            self.assertEqual(result['summary'],summary['summary'])
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
            self.assertEqual(generate.call_count,1)
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

    def test_maxwell_pending_lease_prevents_duplicate_ai_requests(self):
        from threading import Event
        import time
        self.preview(initialize=True)
        index=self.maxwell_index_fixture()
        started,release=Event(),Event()
        def generate(*_):
            started.set()
            release.wait(timeout=5)
            return dict(summary='Fixture summary.',expanded='Fixture expanded summary.')
        before=index.read_bytes()
        with patch.object(self.bible,'_summarize_maxwell_note',side_effect=generate) as model:
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

    def test_maxwell_summary_requires_support_from_the_retrieved_note(self):
        import sys,json
        note=dict(title='Fixture',passage='Galatians 6:1-6',body='Leaders bear responsibility and help others with their burdens in the community.')
        answer=dict(summary='This is a synthetic supported summary with enough words for the validation check.',
                    expanded='This is a synthetic extended summary with enough words to satisfy the supported explanation validation.',
                    supporting_quote='Leaders bear responsibility and help others with their burdens')
        fake=SimpleNamespace(GEMINI_MODELS=['fixture-model'],ask_model=Mock(return_value=json.dumps(answer)))
        with patch.dict(sys.modules,{'ai_assistant':fake}):
            self.assertTrue(self.bible._summarize_maxwell_note(note,'Galatians 6:2','Synthetic NIV verse.'))
            fake.ask_model.assert_called_once()
            self.assertFalse(fake.ask_model.call_args.kwargs['allow_web_fallback'])
            answer['supporting_quote']='These invented words never appeared anywhere inside this commentary'
            fake.ask_model.return_value=json.dumps(answer)
            self.assertIsNone(self.bible._summarize_maxwell_note(note,'Galatians 6:2','Synthetic NIV verse.'))

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
