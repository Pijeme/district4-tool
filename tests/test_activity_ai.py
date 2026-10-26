"""Activity access/retention and concurrent chat behavior, without live services."""
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flask import Response, flash, jsonify

import app as appmod
import ai_assistant as ai
import pastor_resources as resources
import pij_library_knowledge as library
import pij_website_knowledge as website
import developer


class ActivityTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = str(Path(directory.name) / 'app.db')
        for replacement in (
            patch.object(appmod, 'DATABASE', self.database),
            patch.object(appmod, 'sync_from_sheets_if_needed', return_value={}),
            patch.object(resources, '_database_maintenance_active', return_value=False),
            patch.dict(appmod.app.config, TESTING=True, SECRET_KEY='isolated-test-session'),
        ):
            replacement.start()
            self.addCleanup(replacement.stop)
        with appmod.app.app_context():
            appmod.init_db()
        self.client = appmod.app.test_client()
        self.login()

    def login(self, username='pijeme', kind='ao'):
        with self.client.session_transaction() as session:
            session.clear()
            session.update(username=username, name=username.title(), website_activity_csrf='test-csrf')
            if kind == 'ao':
                session.update(ao_logged_in=True, ao_username=username, ao_name=username.title(), ao_role='Area Overseer')
            else:
                session['member_logged_in'] = True

    def rows(self):
        with closing(sqlite3.connect(self.database)) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM website_events ORDER BY id')]

    def seed(self, notification=False, created='2026-10-22T01:00:00+00:00', actor='reader'):
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute('''INSERT INTO website_events
                (created_at,actor_key,actor_username,actor_name,actor_role,action,description,page_path,is_notification)
                VALUES (?,?,?,?,?,?,?,?,?)''', (created, actor, actor, 'Reader', 'Member', 'test', 'Test event', '/bulletin', int(notification)))

    def test_only_signed_in_owner_can_read_or_clear(self):
        self.seed(True)
        for username, kind in [('other', 'ao'), ('pijeme', 'member')]:
            self.login(username, kind)
            for path in ['/ao-tool/activity', '/ao-tool/activity/api?view=summary']:
                self.assertEqual(self.client.get(path).status_code, 403)
            self.assertEqual(self.client.post('/ao-tool/activity/clear', headers={'X-CSRF-Token': 'test-csrf'}).status_code, 403)
        self.assertEqual(len(self.rows()), 1)

    def test_owner_dashboard_renders_and_mutations_require_csrf(self):
        response = self.client.get('/ao-tool/activity')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Clear Activity &amp; Notifications', response.data)
        self.assertIn(b'websiteUpdatesPopup', response.data)
        self.seed(True)
        self.assertEqual(self.client.post('/ao-tool/activity/clear').status_code, 400)
        self.assertEqual(len(self.rows()), 1)

    def test_popup_counts_changes_not_visits_downloads_or_shares(self):
        self.seed(False)
        self.seed(True)
        result = self.client.get('/ao-tool/activity/api?view=summary').get_json()
        self.assertEqual(result['unread'], 1)
        event_id = self.rows()[1]['id']
        response = self.client.post('/ao-tool/activity/read', json={'ids': [event_id]}, headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.get('/ao-tool/activity/api?view=summary').get_json()['unread'], 0)
        self.assertEqual(len(self.rows()), 2)

    def test_visits_are_grouped_and_keep_actual_ao_identity(self):
        with self.client.session_transaction() as session:
            session['pastor_logged_in'] = True
            session['pastor_username'] = 'selected-pastor'
        with patch.dict(appmod.app.view_functions, {'bulletin': lambda: '<h1>Bulletin</h1>'}):
            self.assertEqual(self.client.get('/bulletin').status_code, 200)
        self.assertEqual(self.rows()[0]['actor_username'], 'pijeme')
        result = self.client.get('/ao-tool/activity/api?view=accounts').get_json()
        self.assertEqual(result['items'][0]['activity_count'], 1)
        self.assertEqual(len(self.rows()), 1)  # API polling never becomes a visit.

    def test_successful_changes_only_and_no_credentials(self):
        def success():
            flash('Updated', 'success')
            return Response(status=302, headers={'Location': '/ao-tool'})
        def failure():
            flash('Failed', 'error')
            return Response(status=302, headers={'Location': '/ao-tool'})
        payload = {'username': 'new-user', 'password': 'must-never-be-recorded'}
        with patch.dict(appmod.app.view_functions, {'ao_announcement_create': failure}):
            self.client.post('/ao-tool/announcements/create', data=payload)
        self.assertEqual(self.rows(), [])
        with patch.dict(appmod.app.view_functions, {'ao_announcement_create': success}):
            self.client.post('/ao-tool/announcements/create', data=payload)
        self.assertEqual(len(self.rows()), 1)
        self.assertNotIn('must-never-be-recorded', json.dumps(self.rows()))
        self.assertEqual(self.rows()[0]['is_notification'], 1)

    def test_confirmed_pledge_retry_is_deduplicated(self):
        with patch.dict(appmod.app.view_functions, {'pledge_submit': lambda: jsonify(ok=True, reference='stable-id')}):
            self.client.post('/thanksgiving-pledges/submit')
            self.client.post('/thanksgiving-pledges/submit')
        self.assertEqual(len(self.rows()), 1)

    def test_90_day_retention_keeps_change_notifications(self):
        self.seed(False, '2000-01-01T00:00:00+00:00')
        self.seed(True, '2000-01-01T00:00:00+00:00')
        self.seed(False, '2099-01-01T00:00:00+00:00')
        self.assertEqual(self.client.get('/ao-tool/activity/api?view=activities&account=reader').get_json()['total'], 2)
        with appmod.app.app_context():
            appmod._cleanup_old_visit_logs()
        self.assertEqual(len(self.rows()), 2)

    def test_one_clear_removes_both_histories_but_keeps_accounts(self):
        self.seed(False)
        self.seed(True)
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute("INSERT INTO sheet_accounts_cache(username,name) VALUES ('preserve','Preserve')")
        response = self.client.post('/ao-tool/activity/clear', json={}, headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.rows(), [])
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sheet_accounts_cache').fetchone()[0], 1)

    def test_download_and_share_resolve_book_title_on_server(self):
        book = {'id': 42, 'title': 'Actual title'}
        with patch.object(resources, 'get_effective_book', return_value=book), \
             patch.object(resources, 'is_resource_admin', return_value=False):
            result = self.client.post('/activity/ebook', json={'action': 'share', 'url': '/pastor-resources/download/42', 'title': 'Forged title'})
        self.assertEqual(result.status_code, 200)
        self.assertIn('Actual title', self.rows()[0]['description'])
        self.assertNotIn('Forged', self.rows()[0]['description'])
        self.assertEqual(self.rows()[0]['is_notification'], 0)
        with patch.object(resources, 'get_effective_book', return_value=None):
            self.assertEqual(self.client.post('/activity/ebook', json={'action': 'share', 'url': '/pastor-resources/download/999'}).status_code, 404)
        self.assertEqual(len(self.rows()), 1)


class PijTests(unittest.TestCase):
    def setUp(self):
        self.entered, self.release = threading.Event(), threading.Event()
        self.addCleanup(self.release.set)
        for replacement in (
            patch.object(ai, 'AI_SLOTS', threading.BoundedSemaphore(2)),
            patch.object(ai, 'safe_context_for_gemini', return_value='Authorized context'),
            patch.object(ai, 'library_context_for_gemini', return_value='Library context'),
            patch.object(resources, '_database_maintenance_active', return_value=False),
            patch.dict(appmod.app.config, TESTING=True, SECRET_KEY='isolated-test-session'),
        ):
            replacement.start()
            self.addCleanup(replacement.stop)
        ai.conversation_memory.clear()
        ai.history_touched.clear()
        ai.pending_requests.clear()

    def client(self, username='reader'):
        client = appmod.app.test_client()
        with client.session_transaction() as session:
            session.update(member_logged_in=True, username=username)
        return client

    def test_chat_skips_schema_writes_and_sheet_sync(self):
        with patch.object(ai, 'ask_pij', return_value=('Hello', 'test-model')), \
             patch.object(appmod, 'init_db', side_effect=AssertionError('schema write')), \
             patch.object(appmod, 'sync_from_sheets_if_needed', side_effect=AssertionError('network sync')):
            result = self.client().post('/ai/chat', json={'message': 'Hello'})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.get_json()['answer'], 'Hello')

    def test_about_developer_context_uses_current_public_page_profiles(self):
        with appmod.app.test_request_context('/ai/chat'), \
             patch.object(website, 'authorized_database_snapshot', side_effect=AssertionError('No records needed')):
            context = website.safe_context_for_gemini('unsa ang about developer page?')
        public_data = json.loads(context.split('PUBLIC ABOUT DEVELOPER PAGE DATA\n', 1)[1]
                                 .split('\n\nAUTHORIZED SANITIZED DATABASE SNAPSHOT', 1)[0])
        self.assertEqual(public_data['page'], '/about-developer')
        self.assertEqual([profile['name'] for profile in public_data['developers']],
                         [profile['name'] for profile in developer.DEVELOPERS])
        for actual, expected in zip(public_data['developers'], developer.DEVELOPERS):
            for key, value in actual.items():
                self.assertEqual(value, expected[key])
        self.assertEqual(public_data['voluntary_support']['number'], developer.GCASH_DISPLAY)
        self.assertEqual(public_data['voluntary_support']['name'], developer.GCASH_NAME)
        self.assertNotIn('About Developer is currently Pending', context)
        self.assertIn('Only Mervin', context)
        self.assertIn('Know More', context)
        self.assertIn('Copy GCash Number', context)
        self.assertIn('/about-developer', {rule.rule for rule in appmod.app.url_map.iter_rules()})

    def test_shared_directory_crosses_areas_without_expanding_private_report_scope(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(appmod, 'DATABASE', str(Path(directory) / 'directory.db')):
            with appmod.app.app_context():
                appmod.init_db()
                db = appmod.get_db()
                db.executemany('''INSERT INTO sheet_accounts_cache
                    (username,name,church_address,password,age,sex,contact,birthday,position,sub_area)
                    VALUES (?,?,?,?,?,?,?,?,?,?)''', [
                    ('pastorone', 'Pastor One', 'Address One', 'secret-one', '1', 'Church One',
                     '09111111111', '1990-02-03', 'Pastor', 'A'),
                    ('pastortwo', 'Pastor Two', 'Address Two', 'secret-two', '2', 'Church Two',
                     '09222222222', '1991-04-05', ' Pastor ', 'B'),
                    ('pastorthree', 'Pastor Three', 'Address Three', 'secret-three', '3', 'Church Three',
                     None, None, 'Pastor', 'C'),
                    ('othermember', 'Private Member', 'Home Address', 'secret-member', '1', 'Church One',
                     'private-contact', '1980-01-01', 'Member', 'A'),
                ])
                db.executemany('''INSERT INTO sheet_report_cache
                    (year,month,church,pastor,address,tithes,sheet_row) VALUES(2026,10,?,?,?,?,?)''', [
                    ('Church One', 'Pastor One', 'Address One', 111, 1),
                    ('Church Two', 'Pastor Two', 'Address Two', 222, 2),
                    ('Church Three', 'Pastor Three', 'Address Three', 333, 3),
                ])
                db.commit()

            cases = [
                ({'member_logged_in': True, 'username': 'reader'}, set()),
                ({'pastor_logged_in': True, 'pastor_username': 'pastortwo'}, {'Church Two'}),
                ({'ao_logged_in': True, 'ao_username': 'ao', 'ao_role': 'Area Overseer',
                  'ao_area_number': '2'}, {'Church Two'}),
                ({'ao_logged_in': True, 'ao_username': 'sub', 'ao_role': 'Sub Area Overseer',
                  'ao_area_number': '2', 'ao_sub_area': 'B'}, {'Church Two'}),
                ({'ao_logged_in': True, 'ao_username': 'do', 'ao_role': 'District Overseer'},
                 {'Church One', 'Church Two', 'Church Three'}),
            ]
            for identity, report_churches in cases:
                with self.subTest(identity=identity), appmod.app.test_request_context('/ai/chat'):
                    from flask import session
                    session.update(identity)
                    with patch.object(appmod, 'sync_from_sheets_if_needed', side_effect=AssertionError('No network sync')):
                        snapshot = website.authorized_database_snapshot()
                    rows = snapshot['pastor_church_directory']
                    self.assertEqual([row['area_number'] for row in rows], ['1', '2', '3'])
                    self.assertEqual(rows[0], {
                        'pastor_name': 'Pastor One', 'church_name': 'Church One',
                        'church_address': 'Address One', 'contact_number': '09111111111',
                        'birthday': '1990-02-03', 'area_number': '1', 'sub_area': 'A',
                    })
                    self.assertEqual(rows[2]['contact_number'], '')
                    self.assertEqual(rows[2]['birthday'], '')
                    directory_json = json.dumps(rows)
                    for excluded in ('username', 'password', 'Private Member', 'private-contact', 'Home Address'):
                        self.assertNotIn(excluded, directory_json)
                    self.assertEqual({row['church'] for row in snapshot['report_rows_in_scope']}, report_churches)
                    self.assertNotIn('secret-', json.dumps(snapshot))
                    if identity.get('member_logged_in'):
                        self.assertEqual(snapshot['accounts'], [])

    def test_directory_is_not_exposed_without_authenticated_role_flag(self):
        db = Mock()
        with appmod.app.test_request_context('/ai/chat'):
            from flask import session
            session.update(username='reader', role='Area Overseer')
            self.assertEqual(website._shared_pastor_church_directory(db), [])
        db.execute.assert_not_called()

    def test_directory_question_and_followup_triggers_supply_shared_records(self):
        snapshot = {'pastor_church_directory': [{'pastor_name': 'Pastor One', 'area_number': '1'}]}
        for question in ('Names of the churches of area 1', 'Which churches are in other areas?',
                         'What is his address?', 'What is his phone number?', 'When is his birthday?',
                         'Unsa iyang ngalan?', 'Unsa iyang numero?'):
            with self.subTest(question=question), appmod.app.test_request_context('/ai/chat'), \
                 patch.object(website, 'authorized_database_snapshot', return_value=snapshot) as records:
                context = website.safe_context_for_gemini(question)
                records.assert_called_once_with()
                self.assertIn('"pastor_church_directory"', context)
                self.assertIn('"Pastor One"', context)
                self.assertIn('across ALL areas', context)

    def test_close_during_response_does_not_restore_history_or_allow_overlap(self):
        def answer(*args, **kwargs):
            self.entered.set()
            self.release.wait(5)
            return 'Late response', 'test-model'
        results = []
        with patch.object(ai, 'ask_pij', side_effect=answer):
            worker = threading.Thread(target=lambda: results.append(self.client().post('/ai/chat', json={'message': 'Hello'})))
            worker.start()
            try:
                self.assertTrue(self.entered.wait(3))
                self.assertEqual(self.client().post('/ai/chat', json={'message': 'Again'}).status_code, 409)
                self.assertEqual(self.client().post('/ai/clear', json={}).status_code, 200)
                self.assertEqual(self.client().post('/ai/chat', json={'message': 'Again'}).status_code, 409)
                self.assertEqual(self.client('another').get('/ai/history').get_json()['messages'], [])
            finally:
                self.release.set()
                worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(results[0].status_code, 409)
        self.assertEqual(ai.get_conversation_history('member:reader'), [])
        self.assertEqual(ai.pending_requests, {})
        with patch.object(ai, 'ask_pij', return_value=('Fresh response', 'test-model')):
            self.assertEqual(self.client().post('/ai/chat', json={'message': 'Fresh'}).status_code, 200)

    def test_capacity_is_reserved_before_context_work(self):
        ai.AI_SLOTS.acquire()
        ai.AI_SLOTS.acquire()
        with patch.object(ai, 'safe_context_for_gemini') as context:
            result = self.client().post('/ai/chat', json={'message': 'Hello'})
        self.assertEqual(result.status_code, 429)
        context.assert_not_called()

    def test_provider_errors_are_classified_and_release_capacity(self):
        with patch.object(ai, 'ask_pij', side_effect=RuntimeError('429 RESOURCE_EXHAUSTED')):
            result = self.client().post('/ai/chat', json={'message': 'Hello'})
        self.assertEqual(result.status_code, 429)
        self.assertIn('limit', result.get_json()['error'])
        self.assertIn('request_id', result.get_json())
        self.assertEqual(ai.pending_requests, {})

    def test_empty_answer_falls_back_once_with_bounded_timeout(self):
        first = SimpleNamespace(output_text='')
        second = SimpleNamespace(output_text='Useful answer')
        create = Mock(side_effect=[first, second])
        with patch.object(ai, 'client', SimpleNamespace(interactions=SimpleNamespace(create=create))):
            self.assertEqual(ai.ask_pij('Question')[0], 'Useful answer')
        self.assertEqual(create.call_count, 2)
        self.assertTrue(all(call.kwargs['timeout'] <= ai.AI_REQUEST_SECONDS for call in create.call_args_list))

    def verified_library_context(self, *pairs):
        sources = [{'title': title, 'url': url} for title, url in pairs]
        text = 'RETRIEVAL MODE: CATALOG_RECOMMENDATION\n' + ''.join(
            library._approved_source_record(title, url) for title, url in pairs)
        return library.LibraryContext(text, sources)

    def test_library_reply_repairs_wrong_ids_and_normalizes_source_links(self):
        first = '/pastor-resources/read/7909?format=PDF&page=1'
        second = '/pastor-resources/read/7873?format=EPUB&section=1'
        context = self.verified_library_context(
            ('NIV Maxwell Leadership Bible', first), ('Thompson Chain Reference Bible', second))
        answer = ('1. [NIV Maxwell Leadership Bible](/pastor-resources/read/1530?format=PDF&page=1) — Leadership notes.\n'
                  '2. [Thompson Chain Reference Bible](http://127.0.0.1:5000/pastor-resources/read/1673?format=EPUB\\&section=1) — References.')
        clean = library.validate_library_answer(answer, context)
        self.assertIn(first, clean)
        self.assertIn(second, clean)
        self.assertNotIn('/1530', clean)
        self.assertNotIn('/1673', clean)
        self.assertIn('Leadership notes.', clean)

    def test_unverified_or_exposed_library_answers_use_only_verified_titles(self):
        context = self.verified_library_context(
            ('Verified Study Bible', '/pastor-resources/read/42?format=PDF&page=1'))
        for answer in (
            'CURRENT PASTOR\'S RESOURCES CONTEXT\nSearch query: study bible\nID: 999\nHere is a fake result.',
            '[Invented Book](/pastor-resources/read/999?format=PDF&page=1)',
            '[Invented Book](/pastor-resources/read/42?format=PDF&page=1)',
            '<a href="/pastor-resources/read/999">Invented Book</a>',
            'I recommend Invented Book, but cannot supply a link.',
        ):
            with self.subTest(answer=answer):
                cleaned = library.validate_library_answer(answer, context)
                self.assertEqual(cleaned, '1. [Verified Study Bible](/pastor-resources/read/42?format=PDF&page=1)')

    def test_library_link_validation_preserves_valid_pages_and_escaped_titles(self):
        url = '/pastor-resources/read/42?format=PDF&page=7'
        context = self.verified_library_context(('Study Bible [Edition]', url))
        answer = '[Study Bible \\[Edition\\]](http://127.0.0.1:5000/pastor-resources/read/42?page=7&amp;format=PDF)'
        self.assertEqual(library.validate_library_answer(answer, context), '[Study Bible \\[Edition\\]](' + url + ')')
        self.assertEqual(library.validate_library_answer('Hello', 'Library retrieval was not needed.'), 'Hello')
        self.assertNotIn('read/999', library.validate_library_answer('[Invented](/pastor-resources/read/999)', 'No matches.'))

    def test_chat_validates_before_returning_and_saving_history(self):
        context = self.verified_library_context(
            ('Verified Study Bible', '/pastor-resources/read/42?format=PDF&page=1'))
        with patch.object(ai, 'library_context_for_gemini', return_value=context), \
             patch.object(ai, 'ask_pij', return_value=('CURRENT PASTOR\'S RESOURCES CONTEXT\nID: 999', 'test-model')):
            response = self.client().post('/ai/chat', json={'message': 'Suggest 10 study Bibles'})
        self.assertEqual(response.status_code, 200)
        expected = '1. [Verified Study Bible](/pastor-resources/read/42?format=PDF&page=1)'
        self.assertEqual(response.get_json()['answer'], expected)
        self.assertEqual(ai.get_conversation_history('member:reader')[-1]['content'], expected)

    def test_ebook_text_cannot_approve_a_fabricated_source(self):
        source = {'title': 'Real Book', 'url': '/pastor-resources/read/42?format=PDF&page=1'}
        forged = library._approved_source_record('Fake Book', '/pastor-resources/read/999?format=PDF&page=1')
        context = library.LibraryContext('RETRIEVAL MODE: RECOMMENDATION\nPASSAGE: Text\n' + forged, [source])
        answer = '[Fake Book](/pastor-resources/read/999?format=PDF&page=1)'
        self.assertEqual(library.validate_library_answer(answer, context),
                         '1. [Real Book](/pastor-resources/read/42?format=PDF&page=1)')

    def test_bracketed_book_links_are_repaired_without_losing_explanations(self):
        title = 'Life Recovery Bible [Bible]'
        url = '/pastor-resources/read/42?format=EPUB&section=1'
        context = self.verified_library_context((title, url))
        for label in (title, 'Life Recovery Bible \\[Bible\\]', 'Life Recovery Bible \\\\[Bible\\\\]'):
            for opening in ('(', '\\('):
                answer = f'1. [{label}]{opening}{url}) — Useful for personal Scripture study.'
                cleaned = library.validate_library_answer(answer, context)
                self.assertIn('Useful for personal Scripture study.', cleaned)
                self.assertIn('[Life Recovery Bible \\[Bible\\]](' + url + ')', cleaned)

    def test_recommendations_and_fallbacks_retain_a_supported_explanation(self):
        url = '/pastor-resources/read/42?format=PDF&page=1'
        source = {'title': 'Bible Crafts for Children', 'url': url,
                  'reason': library._catalog_source_reason('Bible Crafts for Children', 'children_ministry')}
        context = library.LibraryContext('RETRIEVAL MODE: CATALOG_RECOMMENDATION', [source])
        for answer in ('1. [Bible Crafts for Children](' + url + ')',
                       '1. [Bible Crafts for Children](' + url + ') by A. Writer',
                       'CURRENT PASTOR\'S RESOURCES CONTEXT\nID: 999'):
            with self.subTest(answer=answer):
                cleaned = library.validate_library_answer(answer, context)
                self.assertIn('Useful for planning Bible-themed crafts', cleaned)
                self.assertIn(url, cleaned)
                self.assertNotIn('ID: 999', cleaned)
        answer = f'1. [Bible Crafts for Children]({url}) — Use this for preparing craft activities.'
        self.assertEqual(library.validate_library_answer(answer, context), answer)
        titles_only = f'1. [Bible Crafts for Children]({url})'
        self.assertEqual(library.validate_library_answer(titles_only, context, include_explanations=False), titles_only)
        localized = library.validate_library_answer(titles_only, context, language='Cebuano')
        self.assertIn('Kapanguhaan alang', localized)
        self.assertNotIn('Useful for', localized)

    def test_topic_fallback_uses_retrieved_excerpt_without_inventing_a_summary(self):
        passage = ('Introduction to unrelated ideas.\n'
                   'Anger can harm relationships when we speak before listening.\n'
                   'Other background material.')
        notes = library._evidence_source_reason(passage, ['anger'])
        self.assertIn('Anger can harm relationships when we speak before listening.', notes['English'])
        self.assertNotIn('Introduction', notes['English'])
        source = {'title': 'Bible Commentary', 'url': '/pastor-resources/read/42?format=PDF&page=7', 'reason': notes}
        context = library.LibraryContext('RETRIEVAL MODE: RECOMMENDATION', [source])
        cleaned = library.validate_library_answer('1. [Bible Commentary](' + source['url'] + ')', context)
        self.assertIn('A relevant excerpt for your topic:', cleaned)
        self.assertIn('Anger can harm relationships', cleaned)
        self.assertNotIn('anger management techniques', cleaned)
        malicious = 'Anger is discussed with [Fake Book](/pastor-resources/read/999) in this passage.'
        self.assertNotIn('/read/999', library._evidence_source_reason(malicious, ['anger'])['English'])

    def test_pdf_line_wrapping_does_not_break_a_fallback_explanation(self):
        passage = 'Prayer helps believers depend on God\nwhile serving the church and caring for people.'
        note = library._evidence_source_reason(passage, ['prayer'])['English']
        self.assertIn('depend on God while serving the church', note)
        self.assertIn('caring for people.', note)

    def test_one_bad_entry_does_not_remove_other_books_explanations(self):
        first = {'title': 'First Study Bible', 'url': '/pastor-resources/read/42?format=PDF&page=1',
                 'reason': {'English': 'A safe fallback note for the first book.'}}
        second = {'title': 'Second Study Bible', 'url': '/pastor-resources/read/43?format=PDF&page=1',
                  'reason': {'English': 'A safe fallback note for the second book.'}}
        context = library.LibraryContext('RETRIEVAL MODE: CATALOG_RECOMMENDATION', [first, second])
        answer = ('1. [First Study Bible](' + first['url'] + ') — Keep this useful, distinct explanation.\n'
                  '2. [Invented Book](/pastor-resources/read/999?format=PDF&page=1) — Unsupported claim.')
        cleaned = library.validate_library_answer(answer, context)
        self.assertIn('Keep this useful, distinct explanation.', cleaned)
        self.assertNotIn('safe fallback note for the first', cleaned)
        self.assertIn('safe fallback note for the second', cleaned)
        self.assertNotIn('Invented Book', cleaned)
        self.assertNotIn('Unsupported claim', cleaned)
        self.assertEqual(cleaned.count('First Study Bible'), 1)

    def test_report_leak_does_not_discard_independently_verified_recommendations(self):
        source = {'title': 'Prayer Guide', 'url': '/pastor-resources/read/42?format=PDF&page=7',
                  'reason': {'English': 'A supported fallback excerpt.'}}
        context = library.LibraryContext('RETRIEVAL MODE: RECOMMENDATION', [source])
        answer = ("CURRENT PASTOR'S RESOURCES CONTEXT\nSearch query: prayer\nID: 999\n"
                  '1. [Prayer Guide](' + source['url'] + ') — Explains dependence on God in daily prayer.')
        cleaned = library.validate_library_answer(answer, context)
        self.assertIn('Explains dependence on God in daily prayer.', cleaned)
        self.assertNotIn('CONTEXT', cleaned)
        self.assertNotIn('999', cleaned)
        self.assertNotIn('supported fallback', cleaned)

    def test_study_bible_fallback_notes_distinguish_editions(self):
        titles = ('Amplified Study Bible', 'Beyond Suffering Bible NLT', 'New Believers Bible',
                  'Life Recovery Bible', 'Chronological Life Application Study Bible',
                  'CSB Ancient Faith Study Bible', 'CSB Apologetics Study Bible',
                  'CSB Apologetics Study Bible for Students', 'CSB Baker Illustrated Study Bible',
                  'CSB Disciples Study Bible')
        notes = [library._catalog_source_reason(title, 'study_bible')['English'] for title in titles]
        self.assertEqual(len(set(notes)), len(titles))
        self.assertFalse(any(note == 'A study Bible option for personal Scripture reading and study.' for note in notes))

    def test_pij_browser_renders_bracketed_book_titles_as_safe_clickable_links(self):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.skipTest('Optional Playwright is required for the chat renderer check')
        template = (Path(__file__).resolve().parents[1] / 'templates' / 'base.html').read_text(encoding='utf-8')
        functions = []
        for name in ('escapeHtml', 'normalizePijInternalHref', 'renderPijInternalLinks', 'formatPijText'):
            start = template.index('      function ' + name + '(')
            end = template.index('\n      function ', start + 1)
            functions.append(template[start:end])
        with sync_playwright() as playwright:
            try:
                browser = playwright.chromium.launch(headless=True)
            except Exception as exc:
                self.skipTest('Optional Chromium unavailable: ' + str(exc).splitlines()[0])
            try:
                page = browser.new_page()
                page.route('http://pij.test/**', lambda route: route.fulfill(body='<html><body></body></html>', content_type='text/html'))
                page.goto('http://pij.test/')
                text = ('1. [Life Bible \\[Edition\\]](/pastor-resources/read/42?format=EPUB&section=1) — Helpful explanation.\n'
                        '2. [Other Bible [Edition]](/pastor-resources/read/43?format=PDF&page=1) — Another explanation.\n'
                        '[Unsafe](javascript:alert(1)) <img src=x onerror=alert(1)>')
                result = page.evaluate('(text) => {' + '\n'.join(functions) + '''
                    document.body.innerHTML = formatPijText(text);
                    return {links: [...document.querySelectorAll('a')].map(a => ({label:a.textContent,href:a.getAttribute('href')})),
                            text:document.body.textContent, images:document.querySelectorAll('img').length};
                }''', text)
                self.assertEqual(len(result['links']), 2)
                self.assertEqual(result['links'][0], {'label': 'Life Bible [Edition]', 'href': '/pastor-resources/read/42?format=EPUB&section=1'})
                self.assertEqual(result['links'][1]['label'], 'Other Bible [Edition]')
                self.assertIn('Helpful explanation.', result['text'])
                self.assertEqual(result['images'], 0)
            finally:
                browser.close()

    def test_installed_sdk_contract_and_no_hidden_retries(self):
        import httpx
        from google import genai
        requests = []
        status = [200]

        def respond(request):
            requests.append(json.loads(request.content))
            if status[0] == 503:
                return httpx.Response(503, json={'error': {'message': 'Unavailable', 'status': 'UNAVAILABLE'}}, request=request)
            return httpx.Response(200, json={'id': 'test', 'status': 'completed', 'steps': [
                {'type': 'model_output', 'content': [{'type': 'text', 'text': 'Contract answer'}]}
            ]}, request=request)

        client = genai.Client(api_key='test-only', http_options={
            'httpx_client': httpx.Client(transport=httpx.MockTransport(respond)),
            'retry_options': {'attempts': 0},
        })
        try:
            with patch.object(ai, 'client', client):
                self.assertEqual(ai.ask_model('gemini-3.5-flash-lite', 'Question', timeout=2), 'Contract answer')
                self.assertEqual(requests[0]['generation_config']['max_output_tokens'], 2400)
                status[0] = 503
                with self.assertRaises(Exception):
                    ai.ask_model('gemini-3.5-flash-lite', 'Question', timeout=2)
                self.assertEqual(len(requests), 2)  # One HTTP call per model attempt.
        finally:
            client.close()

    def test_history_expiry_and_isolated_clear(self):
        ai.save_conversation_message('member:a', 'user', 'First')
        ai.save_conversation_message('member:b', 'user', 'Second')
        ai.clear_conversation('member:a')
        self.assertEqual(ai.get_conversation_history('member:a'), [])
        self.assertEqual(len(ai.get_conversation_history('member:b')), 1)
        ai.history_touched['member:b'] = time.monotonic()-ai.HISTORY_IDLE_SECONDS-1
        self.assertEqual(ai.get_conversation_history('member:b'), [])

    def test_simple_chat_does_not_query_library(self):
        with patch.object(library, '_catalog_specific_book_for_question', side_effect=AssertionError('catalog scan')):
            self.assertIn('not needed', library.library_context_for_gemini('Hello'))

    def test_cancelled_fts_query_does_not_retry_as_a_full_text_scan(self):
        connection = Mock()
        connection.execute.side_effect = sqlite3.OperationalError('interrupted')
        with patch.object(library, '_fts_available', return_value=True):
            with self.assertRaisesRegex(sqlite3.OperationalError, 'interrupted'):
                library._search_chunks(connection, ['giving'])
        self.assertEqual(connection.execute.call_count, 1)

    def test_read_only_retrieval_cannot_write_or_create_databases(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = SimpleNamespace(DATABASE=str(Path(directory)/'main.db'), AI_DATABASE=str(Path(directory)/'ai.db'))
            with closing(sqlite3.connect(paths.DATABASE)) as conn, conn:
                conn.execute('CREATE TABLE marker(value TEXT)')
            with closing(sqlite3.connect(paths.AI_DATABASE)) as conn, conn:
                conn.execute('CREATE TABLE marker(value TEXT)')
            with patch.object(library, '_appmod', return_value=paths), closing(library._db(read_only=True)) as conn:
                with self.assertRaises(sqlite3.OperationalError):
                    conn.execute("INSERT INTO marker VALUES ('not allowed')")

    def test_private_sermon_search_reads_during_writes_without_schema_setup(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = SimpleNamespace(DATABASE=str(Path(directory)/'main.db'), AI_DATABASE=str(Path(directory)/'ai.db'))
            with closing(sqlite3.connect(paths.AI_DATABASE)) as conn, conn:
                conn.execute('CREATE TABLE marker(value TEXT)')
            with closing(sqlite3.connect(paths.DATABASE)) as conn:
                conn.execute('PRAGMA journal_mode=WAL')
                conn.executescript('''CREATE TABLE sermon_library_files(id INTEGER,filename TEXT,manual_theme TEXT,detected_theme TEXT,is_active INTEGER);
                    CREATE TABLE sermon_library_pages(sermon_id INTEGER,page_number INTEGER,page_text TEXT);
                    CREATE TABLE sermon_hidden_items(sermon_id INTEGER);
                    INSERT INTO sermon_library_files VALUES(1,'Faith sermon','','Faith',1);
                    INSERT INTO sermon_library_pages VALUES(1,1,'Faith and hope');''')
                conn.commit()
                conn.execute('BEGIN IMMEDIATE')
                try:
                    with patch.object(library, '_appmod', return_value=paths), \
                         patch.object(library.sermon_ebooks, 'is_sermon_admin', return_value=True), \
                         patch.object(library.sermon_ebooks, 'ensure_sermon_tables', side_effect=AssertionError('Schema write during chat')):
                        rows = library._private_sermon_search('faith', 2)
                        self.assertEqual(rows[0]['sermon_id'], 1)
                finally:
                    conn.rollback()


if __name__ == '__main__':
    unittest.main()
