"""Portability and incremental sync checks use temporary databases and fake Drive."""

import copy
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

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


if __name__ == '__main__':
    unittest.main()
