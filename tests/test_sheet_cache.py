"""Safety checks use temporary SQLite databases and mocked Google Sheets only."""

import copy
import io
import os
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import app as appmod
import area_progress_monitor as monitor
import church_progress as members
import pastor_resources
import schedule
import sheet_cache
import temp_edit


def fixtures():
    headers = {
        name: [item[0] if isinstance(item, tuple) else item for item in required]
        for name, required in sheet_cache.REQUIRED_HEADERS.items()
    }
    headers['Accounts'] += ['Contact #', 'Birth Day', 'Sub Area', 'GooglePinLocation']
    headers['Report'] += ['ReportStatus']
    headers['DistrictSchedule'] += ['Joining', 'Theme', 'Text']
    headers['ChainPrayerSchedules'] += ['Pastor']
    headers['PrayerRequest'] += ["Pastor's Praying", 'Answered Date']
    headers['Anouncement'] += ['SubArea', 'Author Username', 'Author Name']
    def row(name, values):
        return [values.get(header, '') for header in headers[name]]
    accounts = [
        {'Name':'Developer', 'UserName':'Pijeme', 'Password':'test-password', 'Position':'Area Overseer',
         'Area Number':'1', 'Church ID':'DevChurch', 'Church Address':'DevAddress'},
        {'Name':'Pastor One', 'UserName':'pastorone', 'Password':'test-password', 'Position':'Pastor',
         'Area Number':'1', 'Church ID':'ChurchA', 'Church Address':'AddressA'},
        {'Name':'Other AO', 'UserName':'otherao', 'Password':'test-password', 'Position':'Area Overseer',
         'Area Number':'1', 'Church ID':'OtherChurch', 'Church Address':'OtherAddress'},
    ]
    records = {
        'Accounts': accounts,
        'Report': [{'activity_date':'9/6/2026', 'church':'ChurchA', 'pastor':'Pastor One',
                    'address':'AddressA', 'status':'Pending AO approval', 'adult':'12',
                    'tithes':'1,200', 'amount to send':'1,200'}],
        'AOPT': [{'Month':'September 2026','Amount':'75'}],
        'PrayerRequest': [{'Request ID':'req1','Church Name':'ChurchA','Submitted By':'pastorone',
                           'Prayer Request Title':'Test prayer','Prayer Request Date':'2026-10-05',
                           'Prayer Request':'Test body','Status':'Approved'}],
        'DistrictSchedule': [{'Church Name':'ChurchA','Church Address':'AddressA',
                              'Activity Date Start':'2026-10-05','Activity Type':'Fellowship'}],
        'ChainPrayerSchedules': [{'ChurchNameAssigned':'ChurchA','Date':'2026-10-06','Pastor':'Pastor One'}],
        'Anouncement': [{'Title':'Greeting','Announcement':'Welcome','Date':'2026-10-05','Area':'1'}],
        'Members Account': [{'Name':'Alice','BDay':'2000-01-01','Church ID':'ChurchA',
                             'Church Address':'AddressA','Area Number':'1','Pastor':'Pastor One',
                             'UserName':'alice','Password':'test-password'}],
    }
    return {name:[headers[name]]+[row(name,item) for item in records[name]] for name in headers}


class FakeSheets:
    def __init__(self, values=None, failed=None):
        self.values = copy.deepcopy(values if values is not None else fixtures())
        self.failed = failed
        self.reads = []

    def open(self, title):
        if title != 'District4 Data':
            raise AssertionError(title)
        return self

    def worksheet(self, name):
        def read():
            self.reads.append(name)
            if name == self.failed:
                raise RuntimeError('Simulated network failure')
            return copy.deepcopy(self.values[name])
        return Mock(get_all_values=read)


class CacheSafetyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='district4_cache_test_')
        self.addCleanup(self.directory.cleanup)
        self.path = os.path.join(self.directory.name, 'app_v2.db')
        self.patches = [patch.object(appmod,'DATABASE',self.path),
                        patch.object(appmod,'AI_DATABASE',os.path.join(self.directory.name,'ai_index.db')),
                        patch.dict(appmod.app.config, DATABASE=self.path, TESTING=True),
                        patch.object(appmod.app.logger, 'exception')]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)
        # sqlite's context manager commits/rolls back but does not close. Track
        # connections from existing route helpers so Windows cleanup is reliable.
        self.connections=[]
        original_connect=sqlite3.connect
        def tracked_connect(*args,**kwargs):
            connection=original_connect(*args,**kwargs)
            self.connections.append(connection)
            return connection
        tracker=patch.object(sqlite3,'connect',side_effect=tracked_connect)
        tracker.start()
        self.addCleanup(tracker.stop)
        self.addCleanup(lambda:[connection.close() for connection in self.connections])
        with appmod.app.app_context():
            appmod.init_db()
            appmod.get_db().execute('CREATE TABLE unrelated_state (value TEXT)')
            appmod.get_db().execute("INSERT INTO unrelated_state VALUES ('keep')")
            appmod.get_db().commit()
            self.sync(FakeSheets())
        self.client = appmod.app.test_client()
        self.no_google = patch.object(appmod,'get_gs_client',side_effect=AssertionError('Unexpected Google Sheets read'))
        self.google = self.no_google.start()
        self.addCleanup(self.no_google.stop)

    def sync(self, fake, names=None):
        return sheet_cache.sync_cache(appmod.get_db(), self.path, lambda:fake,
                                      appmod.parse_float, appmod.parse_sheet_date,
                                      lambda:'2026-10-05T10:00:00+00:00', names)

    def snapshot(self):
        with sqlite3.connect(self.path) as db:
            return {table: db.execute(f'SELECT * FROM {table}').fetchall()
                    for table in [*sheet_cache.DATASETS.values(),'sheet_cache_sync_state','sync_state','unrelated_state']}

    def login(self, username='Pijeme', role='Area Overseer', pastor=False):
        with self.client.session_transaction() as state:
            state.clear()
            if pastor:
                state.update(pastor_logged_in=True,pastor_username='pastorone',pastor_name='Pastor One',
                             pastor_church_id='ChurchA',pastor_church_address='AddressA',pastor_area_number='1')
            else:
                state.update(ao_logged_in=True,ao_username=username,ao_role=role,ao_area_number='1',ao_name='Test AO')
            state['cache_control_token']='test-csrf-token'

    def test_full_sync_fetches_exact_eight_and_preserves_unrelated_tables(self):
        fake = FakeSheets()
        with appmod.app.app_context():
            counts = self.sync(fake)
        self.assertEqual(fake.reads,list(sheet_cache.DATASETS))
        self.assertEqual(counts['Accounts'],3)
        self.assertEqual(counts['Members Account'],1)
        self.assertEqual(self.snapshot()['unrelated_state'],[('keep',)])

    def test_last_fetch_failure_keeps_every_cache_and_timestamps(self):
        before = self.snapshot()
        with appmod.app.app_context(), self.assertRaises(sheet_cache.CacheSyncError):
            self.sync(FakeSheets(failed='Members Account'))
        self.assertEqual(self.snapshot(),before)

    def test_missing_headers_or_blank_response_keeps_all_caches(self):
        for invalid in ([],[['Wrong header']], [['Name','Name']]):
            with self.subTest(invalid=invalid):
                before = self.snapshot()
                fake = FakeSheets()
                fake.values['Members Account']=invalid
                with appmod.app.app_context(), self.assertRaises(sheet_cache.CacheSyncError):
                    self.sync(fake)
                self.assertEqual(self.snapshot(),before)

    def test_insert_failure_rolls_back_all_eight_and_sync_state(self):
        before = self.snapshot()
        with appmod.app.app_context():
            db=appmod.get_db()
            db.execute("""CREATE TRIGGER fail_members BEFORE INSERT ON sheet_members_account_cache
                          BEGIN SELECT RAISE(ABORT,'simulated insert failure'); END""")
            db.commit()
            with self.assertRaises(sheet_cache.CacheSyncError):
                self.sync(FakeSheets())
        self.assertEqual(self.snapshot(),before)

    def test_targeted_refresh_fetches_and_changes_only_requested_dataset(self):
        before=self.snapshot()
        fake=FakeSheets()
        fake.values['AOPT'][1][1]='100'
        with appmod.app.app_context():
            self.sync(fake,['AOPT'])
        after=self.snapshot()
        self.assertEqual(fake.reads,['AOPT'])
        for table in sheet_cache.DATASETS.values():
            if table!='sheet_aopt_cache':
                self.assertEqual(after[table],before[table])
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT amount FROM sheet_aopt_cache').fetchone()[0],100)
        self.assertEqual(after['sync_state'],before['sync_state'])

    def test_header_only_empty_sheet_is_valid_and_does_not_repeat_api_reads(self):
        fake=FakeSheets()
        fake.values['DistrictSchedule']=fake.values['DistrictSchedule'][:1]
        fake.values['ChainPrayerSchedules']=fake.values['ChainPrayerSchedules'][:1]
        with appmod.app.app_context():
            self.sync(fake,['DistrictSchedule','ChainPrayerSchedules'])
            for _ in range(3):
                appmod.ensure_schedule_cache_loaded()
                appmod.sync_from_sheets_if_needed()
        self.google.assert_not_called()

    def test_legacy_header_aliases_are_supported(self):
        fake=FakeSheets()
        for old,new in [('Area Number','Age'),('Church ID','Sex'),('Sub Area','SubArea')]:
            headers=fake.values['Accounts'][0]
            headers[headers.index(old)]=new
        headers=fake.values['Members Account'][0]
        headers[headers.index('BDay')]='Birthday'
        with appmod.app.app_context():
            self.sync(fake,['Accounts','Members Account'])
        self.assertEqual(self.snapshot()['sheet_members_account_cache'][0][2],'2000-01-01')

    def test_invalid_report_date_and_duplicate_account_fail_before_deletes(self):
        for target in ('date','duplicate'):
            before=self.snapshot()
            fake=FakeSheets()
            if target=='date':
                fake.values['Report'][1][0]='not a date'
            else:
                fake.values['Accounts'].append(fake.values['Accounts'][1])
            with appmod.app.app_context(), self.assertRaises(sheet_cache.CacheSyncError):
                self.sync(fake)
            self.assertEqual(self.snapshot(),before)

    def test_confirmed_write_failed_refresh_queues_targeted_retry(self):
        self.login()
        before=self.snapshot()
        with appmod.app.test_request_context('/'), patch.object(appmod,'get_gs_client',return_value=FakeSheets(failed='AOPT')):
            self.assertFalse(appmod.refresh_after_sheet_write('AOPT'))
            self.assertEqual(appmod.get_db().execute('SELECT dataset FROM sheet_cache_pending_refresh').fetchall()[0][0],'AOPT')
        self.assertEqual(self.snapshot(),before)
        fake=FakeSheets()
        with appmod.app.app_context(), patch.object(appmod,'get_gs_client',return_value=fake):
            appmod.sync_from_sheets_if_needed()
            self.assertEqual(appmod.get_db().execute('SELECT COUNT(*) FROM sheet_cache_pending_refresh').fetchone()[0],0)
        self.assertEqual(fake.reads,['AOPT'])

    def test_bootstrap_fetches_only_new_members_cache_for_existing_installation(self):
        with appmod.app.app_context():
            db=appmod.get_db()
            db.execute("DELETE FROM sheet_cache_sync_state WHERE dataset='Members Account'")
            db.commit()
            fake=FakeSheets()
            with patch.object(appmod,'get_gs_client',return_value=fake):
                appmod.sync_from_sheets_if_needed()
                appmod.sync_from_sheets_if_needed()
        self.assertEqual(fake.reads,['Members Account'])

    def test_controls_require_developer_identity_role_and_token(self):
        routes=['/ao-tool/cache/full-sync','/ao-tool/cache/download-db']
        for route in routes:
            with self.client.session_transaction() as state:
                state.clear()
            self.assertEqual(self.client.post(route).status_code,403)
            self.login('otherao')
            self.assertEqual(self.client.post(route,data={'cache_control_token':'test-csrf-token'}).status_code,403)
            self.login('Pijeme','Sub Area Overseer')
            self.assertEqual(self.client.post(route,data={'cache_control_token':'test-csrf-token'}).status_code,403)
            self.login()
            self.assertEqual(self.client.post(route).status_code,400)
            self.assertEqual(self.client.post(route,data={'cache_control_token':'wrong'}).status_code,400)
        self.google.assert_not_called()

    def test_cached_role_revocation_blocks_backup(self):
        self.login()
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE sheet_accounts_cache SET position='Pastor' WHERE username='Pijeme'")
        response=self.client.post('/ao-tool/cache/download-db',data={'cache_control_token':'test-csrf-token'})
        self.assertEqual(response.status_code,403)

    def test_only_developer_sees_controls(self):
        self.login()
        response=self.client.get('/ao-tool')
        self.assertEqual(response.status_code,200)
        self.assertIn(b'Download Local DB',response.data)
        self.login('otherao')
        response=self.client.get('/ao-tool')
        self.assertNotIn(b'Download Local DB',response.data)
        self.assertNotIn(b'/ao-tool/cache/full-sync',response.data)

    def test_developer_full_sync_is_transactional_and_reports_counts(self):
        self.login()
        fake=FakeSheets()
        with patch.object(appmod,'get_gs_client',return_value=fake):
            response=self.client.post('/ao-tool/cache/full-sync',data={'cache_control_token':'test-csrf-token'},follow_redirects=True)
        self.assertEqual(response.status_code,200)
        self.assertIn(b'Members Account: 1',response.data)
        self.assertEqual(fake.reads,list(sheet_cache.DATASETS))

    def test_download_is_consistent_sqlite_snapshot_including_wal(self):
        self.login()
        with sqlite3.connect(self.path) as live:
            live.execute('PRAGMA journal_mode=WAL')
            live.execute("INSERT INTO unrelated_state VALUES ('committed-in-wal')")
            live.commit()
            response=self.client.post('/ao-tool/cache/download-db',data={'cache_control_token':'test-csrf-token'})
            self.assertEqual(response.status_code,200)
            self.assertIn('no-store',response.headers['Cache-Control'])
            self.assertIn('district4_app_v2_backup_',response.headers['Content-Disposition'])
            self.assertTrue(response.data.startswith(b'SQLite format 3\x00'))
            backup=Path(self.directory.name)/'download.db'
            backup.write_bytes(response.data)
            response.close()
            with sqlite3.connect(backup) as db:
                self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0],'ok')
                self.assertEqual(db.execute('SELECT value FROM unrelated_state').fetchall(),[('keep',),('committed-in-wal',)])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM sheet_accounts_cache').fetchone()[0],3)
                self.assertFalse(db.execute("SELECT name FROM sqlite_master WHERE name='pij_library_chunks'").fetchall())

    def test_normal_login_and_page_flows_use_sqlite(self):
        for route in ('/','/ao-login','/pastor-login'):
            response=self.client.post(route,data={'username':'pastorone' if route!='/ao-login' else 'Pijeme','password':'test-password'})
            self.assertEqual(response.status_code,302,route)
        self.login(pastor=True)
        for route in ('/schedules','/prayer-request/status','/prayer-request/answered','/church-progress/ChurchA',
                      '/pastor-tool?year=2026&month=9'):
            response=self.client.get(route)
            self.assertEqual(response.status_code,200,route)
        self.login()
        for route in ('/ao-tool','/ao-tool/church-status','/ao-tool/prayer-requests',
                      '/ao-tool/area-progress-monitor','/api/ao-tool/area-progress-monitor/snapshot'):
            self.assertEqual(self.client.get(route).status_code,200,route)
        self.assertEqual(self.client.post('/api/ao-tool/area-progress-monitor/boot').status_code,200)
        self.google.assert_not_called()

    def test_legacy_login_history_preserves_events_and_allows_pastor_login(self):
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TABLE user_login_events')
            db.execute('''CREATE TABLE user_login_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, name TEXT,
                role TEXT, church_id TEXT, church_address TEXT, area_number TEXT,
                sub_area TEXT, logged_in_at TEXT NOT NULL, ip_address TEXT, user_agent TEXT)''')
            db.execute('''INSERT INTO user_login_events
                (username, name, role, area_number, logged_in_at)
                VALUES ('oldpastor', 'Old Pastor', 'Pastor', '1', '2020-01-01T00:00:00+00:00')''')
        # Legacy schemas migrate before serving requests, including on Gunicorn.
        with appmod.app.app_context():
            appmod.init_db()
        for route in ('/', '/pastor-login'):
            with self.subTest(route=route):
                with self.client.session_transaction() as state:
                    state.clear()
                response = self.client.post(route, data={
                    'username': 'pastorone', 'password': 'test-password'})
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers['Location'], '/bulletin')
                self.assertEqual(self.client.get('/bulletin').status_code, 200)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('''SELECT id, full_name, created_at
                FROM user_login_events WHERE username='oldpastor' ''').fetchone(),
                (1, 'Old Pastor', '2020-01-01T00:00:00+00:00'))
            self.assertEqual(db.execute('''SELECT COUNT(*) FROM user_login_events
                WHERE username='pastorone' ''').fetchone()[0], 1)
        self.google.assert_not_called()

    def test_login_notification_reads_the_event_written_by_login(self):
        self.assertEqual(self.client.post('/', data={
            'username': 'pastorone', 'password': 'test-password'}).status_code, 302)
        with appmod.app.app_context():
            monitor.ensure_area_progress_monitor_tables()
            items = monitor._pastor_login_notifications(
                monitor.Scope(area='1', sub_area='', role='area overseer'),
                {'ChurchA': {'username': 'pastorone', 'pastor_name': 'Pastor One',
                             'church_name': 'ChurchA'}})
        self.assertEqual(len(items), 1)
        self.assertIn('Pastor One', items[0]['message'])

    def test_partial_login_history_upgrade_preserves_required_legacy_fields(self):
        with sqlite3.connect(self.path) as db:
            db.execute('ALTER TABLE user_login_events ADD COLUMN logged_in_at TEXT NOT NULL DEFAULT \'\'')
            db.execute('ALTER TABLE user_login_events ADD COLUMN name TEXT NOT NULL DEFAULT \'\'')
            db.execute('''INSERT INTO user_login_events
                (created_at, username, role, logged_in_at, name)
                VALUES ('', 'oldpastor', 'Pastor', '2020-01-01T00:00:00+00:00', 'Old Pastor')''')
        self.assertEqual(self.client.post('/', data={
            'username': 'pastorone', 'password': 'test-password'}).status_code, 302)
        with appmod.app.app_context():
            monitor.ensure_area_progress_monitor_tables()
            appmod.init_db()
        with sqlite3.connect(self.path) as db:
            old = db.execute('''SELECT created_at, full_name FROM user_login_events
                WHERE username='oldpastor' ''').fetchone()
            self.assertEqual(old, ('2020-01-01T00:00:00+00:00', 'Old Pastor'))
            event = db.execute('''SELECT created_at, logged_in_at, full_name, name
                FROM user_login_events WHERE username='pastorone' ''').fetchone()
            self.assertIsNotNone(event)
            self.assertTrue(event[0])
            self.assertEqual(event[0], event[1])
            self.assertEqual(event[2:], ('Pastor One', 'Pastor One'))

    def test_monitor_can_initialize_login_history_before_authentication(self):
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TABLE user_login_events')
        with appmod.app.app_context():
            monitor.ensure_area_progress_monitor_tables()
        for username, is_pastor in (('Pijeme', False), ('otherao', False), ('pastorone', True)):
            with self.subTest(username=username):
                with self.client.session_transaction() as state:
                    state.clear()
                response = self.client.post('/', data={
                    'username': username, 'password': 'test-password'})
                self.assertEqual(response.status_code, 302)
                self.assertEqual(self.client.get('/bulletin').status_code, 200)
                with self.client.session_transaction() as state:
                    self.assertEqual(bool(state.get('pastor_logged_in')), is_pastor)
                    self.assertEqual(bool(state.get('ao_logged_in')), not is_pastor)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT username FROM user_login_events').fetchall(), [('pastorone',)])

    def test_login_history_failure_does_not_break_authenticated_redirect(self):
        for route in ('/', '/pastor-login'):
            with self.subTest(route=route):
                with self.client.session_transaction() as state:
                    state.clear()
                with sqlite3.connect(self.path) as db:
                    db.execute('''CREATE TRIGGER IF NOT EXISTS fail_login_event
                        BEFORE INSERT ON user_login_events
                        BEGIN SELECT RAISE(ABORT, 'simulated history failure'); END''')
                response = self.client.post(route, data={
                    'username': 'pastorone', 'password': 'test-password'})
                self.assertEqual(response.status_code, 302)
                self.assertEqual(self.client.get('/bulletin').status_code, 200)
                with self.client.session_transaction() as state:
                    self.assertTrue(state['pastor_logged_in'])
                    self.assertEqual(state['pastor_username'], 'pastorone')
        self.assertEqual(appmod.app.logger.exception.call_count, 2)

    def test_invalid_password_never_creates_session_or_login_history(self):
        for route in ('/', '/pastor-login', '/ao-login'):
            response = self.client.post(route, data={
                'username': 'Pijeme' if route == '/ao-login' else 'pastorone',
                'password': 'wrong-password'})
            self.assertEqual(response.status_code, 200)
            with self.client.session_transaction() as state:
                self.assertFalse(state.get('pastor_logged_in'))
                self.assertFalse(state.get('ao_logged_in'))
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM user_login_events').fetchone()[0], 0)

    def test_members_scope_reads_cache_without_live_sheet_client(self):
        with appmod.app.app_context(), patch.object(members,'_get_gs_client',side_effect=AssertionError('page read')):
            found=members._members_from_cache({'church_name':'ChurchA','church_address':'AddressA'})
            self.assertEqual(found[0]['username'],'alice')
            scope=monitor.Scope(area='1',sub_area='',role='area overseer')
            churches=monitor._fetch_scope_churches(scope)
            result=monitor._members_for_scope_from_cache(scope,churches)
            self.assertEqual(result[0]['name'],'Alice')

    def test_failed_account_source_write_does_not_create_local_account(self):
        self.login('otherao')
        data={'full_name':'New Pastor','sex':'ChurchNew','church_address':'New Address',
              'contact_number':'123','birthday':'2000-01-01'}
        before=self.snapshot()
        with patch.object(appmod,'append_account_to_sheet',side_effect=RuntimeError('source write failed')):
            response=self.client.post('/ao-tool/create-account',data=data)
        self.assertEqual(response.status_code,200)
        self.assertIn(b'Unable to save the account to Google Sheets',response.data)
        self.assertEqual(self.snapshot(),before)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM pastors').fetchone()[0],0)

    def test_failed_report_approval_does_not_change_cache(self):
        self.login()
        before=self.snapshot()
        with patch.object(appmod,'sheet_batch_update_status_for_church_month',side_effect=RuntimeError('source write failed')):
            response=self.client.post('/ao-tool/church-status/approve',data={'year':'2026','month':'9','church':'ChurchA'},
                                      headers={'X-Requested-With':'XMLHttpRequest'})
        self.assertFalse(response.get_json()['ok'])
        self.assertEqual(self.snapshot(),before)

    def test_public_static_images_have_ten_day_cache(self):
        response=self.client.get('/static/pij/pij-avatar.png')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.cache_control.max_age,864000)
        self.assertTrue(response.cache_control.public)
        response.close()

    def test_public_study_font_is_cached_without_affecting_private_responses(self):
        response=self.client.get('/static/bible-study/fonts/Rye-Regular.woff')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.cache_control.max_age,864000)
        self.assertTrue(response.cache_control.public)
        response.close()

    def test_private_selfie_is_no_store(self):
        with appmod.app.app_context():
            temp_edit._ensure_temp_edit_tables()
            appmod.get_db().execute("""INSERT INTO temp_edit_requests
                (batch_id,editor_name,selfie_blob,selfie_mime,submitted_date,submitted_time,area_number,church_id)
                VALUES ('test-batch','Editor',?, 'image/jpeg','2026-10-05','10:00','1','ChurchA')""",(b'fake-jpeg',))
            appmod.get_db().commit()
        with patch.object(temp_edit,'_authorized',return_value=True):
            response=self.client.get('/temp-edit-selfie/test-batch?token=test')
        self.assertEqual(response.status_code,200)
        self.assertIn('no-store',response.headers['Cache-Control'])
        self.assertNotIn('public',response.headers['Cache-Control'])
        response.close()

    def test_pastor_thumbnail_revalidates_local_cover(self):
        self.login(pastor=True)
        thumbnail=Path(self.directory.name)/'cover.png'
        thumbnail.write_bytes(b'fake-thumbnail')
        with patch.object(pastor_resources,'get_effective_book',return_value={'title':'Test'}), \
             patch.object(pastor_resources,'get_cached_thumbnail',return_value=(str(thumbnail),'image/png')):
            response=self.client.get('/pastor-resources/thumbnail/1')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.cache_control.max_age,0)
        response.close()

    def test_stale_account_row_is_rejected_before_update_and_queued_for_refresh(self):
        self.login()
        source=fixtures()['Accounts']
        ws=Mock()
        ws.get_all_values.return_value=source
        ws.row_values.return_value=source[3]  # A different account moved into row 3.
        fake=Mock()
        fake.open.return_value.worksheet.return_value=ws
        with appmod.app.app_context(), patch.object(appmod,'get_gs_client',return_value=fake), \
             self.assertRaisesRegex(RuntimeError,'data changed'):
            appmod._update_account_in_sheet('pastorone',{'full_name':'Changed'})
        ws.update.assert_not_called()
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT dataset FROM sheet_cache_pending_refresh').fetchone()[0],'Accounts')
            self.assertEqual(db.execute("SELECT name FROM sheet_accounts_cache WHERE username='pastorone'").fetchone()[0],'Pastor One')

    def test_source_row_identity_check_accepts_matching_dates_in_different_formats(self):
        with appmod.app.app_context():
            values=fixtures()['Report']
            appmod.verify_sheet_row(Mock(),'Report',2,values=values)
            values[1][0]='2026-09-06'
            appmod.verify_sheet_row(Mock(),'Report',2,values=values)

    def test_report_approval_writes_source_before_refreshing_report_only(self):
        self.login()
        events=[]
        def source_write(*args):
            events.append('source-write')
        def cache_refresh(*names):
            events.append(names)
            return True
        with patch.object(appmod,'sheet_batch_update_status_for_church_month',side_effect=source_write), \
             patch.object(appmod,'refresh_after_sheet_write',side_effect=cache_refresh):
            response=self.client.post('/ao-tool/church-status/approve',data={'year':'2026','month':'9','church':'ChurchA'},
                                      headers={'X-Requested-With':'XMLHttpRequest'})
        self.assertTrue(response.get_json()['ok'])
        self.assertEqual(events,['source-write',('Report',)])

    def test_member_create_writes_source_and_refreshes_members_only(self):
        self.login(pastor=True)
        events=[]
        ws=Mock()
        ws.append_row.side_effect=lambda *args,**kwargs:events.append('source-write')
        def refresh(*names):
            events.append(names)
            return True
        with patch.object(members,'_get_members_ws',return_value=ws), \
             patch.object(members,'_generate_username',return_value='newmember'), \
             patch.object(appmod,'refresh_after_sheet_write',side_effect=refresh):
            response=self.client.post('/church-progress/ChurchA/member/create',json={'name':'New Member','bday':'2000-01-01'})
        self.assertTrue(response.get_json()['ok'])
        self.assertEqual(events,['source-write',('Members Account',)])

    def test_failed_member_source_write_does_not_refresh_or_change_cache(self):
        self.login(pastor=True)
        before=self.snapshot()
        ws=Mock()
        ws.append_row.side_effect=RuntimeError('source failure')
        with patch.object(members,'_get_members_ws',return_value=ws), \
             patch.object(members,'_generate_username',return_value='newmember'), \
             patch.object(appmod,'refresh_after_sheet_write') as refresh:
            response=self.client.post('/church-progress/ChurchA/member/create',json={'name':'New Member','bday':'2000-01-01'})
        self.assertEqual(response.status_code,500)
        self.assertFalse(response.get_json()['ok'])
        refresh.assert_not_called()
        self.assertEqual(self.snapshot(),before)

    def test_schedule_update_verifies_target_then_refreshes_district_only(self):
        self.login()
        events=[]
        ws=Mock()
        def update(*args,**kwargs):
            events.append('source-write')
        ws.update.side_effect=update
        with appmod.app.test_request_context('/schedules'), \
             patch.object(schedule,'_ensure_district_schedule_headers',return_value=ws), \
             patch.object(appmod,'verify_sheet_row',side_effect=lambda *args,**kwargs:events.append('verify-row')), \
             patch.object(appmod,'queue_sheet_cache_refresh',side_effect=lambda *names:events.append(names)):
            schedule._update_district_schedule_row(2,{'church_name':'ChurchA'})
        self.assertEqual(events,['verify-row','source-write',('DistrictSchedule',)])

    def test_failed_report_export_does_not_mark_month_submitted(self):
        self.login(pastor=True)
        with appmod.app.test_request_context('/pastor-tool'):
            appmod.get_or_create_monthly_report(2026,10,'pastorone')
            job_id=appmod._create_submit_report_job('pastorone',2026,10)
            with patch.object(appmod,'refresh_sheet_cache',return_value={}), \
                 patch.object(appmod,'_report_exists_for_pastor_month_from_cache',return_value=False), \
                 patch.object(appmod,'_export_month_to_sheet_for_pastor',side_effect=RuntimeError('source failure')):
                appmod._process_submit_report_job(job_id)
            self.assertEqual(appmod.get_db().execute('SELECT submitted FROM monthly_reports').fetchone()[0],0)
            self.assertEqual(appmod._get_submit_report_job(job_id)['status'],'failed')

    def test_database_recovery_bootstraps_all_eight_sources(self):
        with appmod.app.app_context():
            db=appmod.get_db()
            db.execute('DELETE FROM sheet_cache_sync_state')
            db.execute('UPDATE sync_state SET last_sync=NULL WHERE id=1')
            for table in sheet_cache.DATASETS.values():
                db.execute(f'DELETE FROM {table}')
            db.commit()
            fake=FakeSheets()
            with patch.object(appmod,'get_gs_client',return_value=fake):
                appmod.sync_from_sheets_if_needed()
            self.assertEqual(fake.reads,list(sheet_cache.DATASETS))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM sheet_accounts_cache').fetchone()[0],3)

    def test_failed_full_sync_route_reports_failure_and_keeps_caches(self):
        self.login()
        before=self.snapshot()
        with patch.object(appmod,'get_gs_client',return_value=FakeSheets(failed='Members Account')):
            response=self.client.post('/ao-tool/cache/full-sync',data={'cache_control_token':'test-csrf-token'},follow_redirects=True)
        self.assertIn(b'Sync failed',response.data)
        self.assertEqual(self.snapshot(),before)

    def test_full_sync_keeps_recovery_request_created_during_fetch(self):
        fake=FakeSheets()
        original_worksheet=fake.worksheet
        def worksheet(name):
            ws=original_worksheet(name)
            if name=='Members Account':
                read=ws.get_all_values
                def record_concurrent_write():
                    appmod.queue_sheet_cache_refresh('Accounts')
                    return read()
                ws.get_all_values=record_concurrent_write
            return ws
        fake.worksheet=worksheet
        with appmod.app.app_context():
            self.sync(fake)
            self.assertEqual(appmod.get_db().execute('SELECT dataset FROM sheet_cache_pending_refresh').fetchone()[0],'Accounts')


class CCMValidityTests(unittest.TestCase):
    """Expiration survives local saves and Google backup without shifting old columns."""

    def setUp(self):
        from PIL import Image
        directory = tempfile.TemporaryDirectory(prefix='district4_ccm_test_')
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.now = datetime(2026, 11, 6, 16, 0, tzinfo=timezone.utc)  # Nov 7 in Manila.
        for replacement in (
            patch.object(appmod, 'DATABASE', str(self.directory / 'app.db')),
            patch.dict(os.environ, DATA_DIR=str(self.directory)),
            patch.dict(appmod.app.config, TESTING=True, SECRET_KEY='ccm-test'),
            patch.object(appmod, 'utc_now', return_value=self.now),
            patch.object(appmod, 'get_gs_client', side_effect=AssertionError('Unexpected Google request')),
            patch.object(appmod, 'ccm_churches', return_value=[{
                'key': 'church', 'area': '1', 'church_id': 'C1', 'address': 'Church address', 'pastor': 'Pastor One'}]),
        ):
            replacement.start()
            self.addCleanup(replacement.stop)
        with appmod.app.app_context():
            appmod.init_db()
            appmod.ccm_cache_write([])
        self.client = appmod.app.test_client()
        self.key = 'a' * 32
        with self.client.session_transaction() as state:
            state.update(ccm_admin_until=appmod.time.time() + 3600, ccm_csrf='csrf',
                         ccm_draft={'key': self.key, 'created': self.now.isoformat()})
        buffer = io.BytesIO()
        Image.new('RGB', (400, 400), '#cbdcec').save(buffer, 'PNG')
        self.photo = buffer.getvalue()

    def form(self):
        return dict(csrf_token='csrf', form_key=self.key, name='Worker One', area='1', church_key='church',
                    photo=(io.BytesIO(self.photo), 'photo.png'))

    def save(self):
        response = self.client.post('/ccm/admin/save', data=self.form())
        self.assertEqual(response.status_code, 200, response.data)
        with appmod.app.app_context():
            return appmod.ccm_pending()[self.key]

    def test_preview_and_save_assign_expiration_and_ignore_posted_overrides(self):
        for endpoint in ('preview', 'save'):
            with self.subTest(endpoint=endpoint), \
                 patch.object(appmod, 'ccm_render_id', wraps=appmod.ccm_render_id) as render:
                data = self.form()
                data['expiration_date'] = '2099-01-01'
                response = self.client.post('/ccm/admin/' + endpoint, data=data)
                self.assertEqual(response.status_code, 200, response.data)
                self.assertTrue(response.json['ok'])
                self.assertEqual(render.call_args.args[0]['Expiration Date'], '2027-11-07')

    def test_expiration_uses_calendar_anniversary_and_local_creation_date(self):
        for created, expected in [
            ('2026-11-06T16:00:00+00:00', '2027-11-07'),
            ('2023-03-01T00:00:00+08:00', '2024-03-01'),
            ('2024-02-28T16:00:00+00:00', '2025-02-28'),
            ('2024-12-31T16:00:00+00:00', '2026-01-01'),
        ]:
            with self.subTest(created=created):
                self.assertEqual(appmod.ccm_id_expiration(created), expected)

    def test_validity_includes_expiration_day_in_manila_and_preserves_legacy(self):
        for expiration, expired in [('2026-11-06', True), ('2026-11-07', False),
                                    ('2026-11-08', False), ('', False), (None, False), ('bad-date', True)]:
            with self.subTest(expiration=expiration):
                self.assertEqual(appmod.ccm_expired(expiration), expired)
        record = self.save()
        with appmod.app.app_context():
            for expiration, expected in [('2026-11-06', b'Expired worker ID'),
                                         ('2026-11-07', b'Active worker ID'), ('', b'Active worker ID')]:
                record['Expiration Date'] = expiration
                with patch.object(appmod, 'ccm_pending', return_value={self.key: record}):
                    response = self.client.get('/ccm/verify/' + self.key)
                self.assertEqual(response.status_code, 200)
                self.assertIn(expected, response.data)
            record['Status'] = 'Inactive'
            with patch.object(appmod, 'ccm_pending', return_value={self.key: record}):
                self.assertIn(b'Inactive worker ID', self.client.get('/ccm/verify/' + self.key).data)

    def test_save_edit_and_pages_preserve_expiration_and_identity(self):
        record = self.save()
        self.assertEqual(record['Expiration Date'], '2027-11-07')
        for path in ('/ccm?tab=workers', '/ccm/saved/' + self.key, '/ccm/print/' + self.key):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Nov 07, 2027', response.data)
            response.close()
        response = self.client.get('/ccm?tab=admin&edit=' + self.key)
        self.assertIn(b'id="ccmExpirationDate" type="text" readonly value="Nov 07, 2027"', response.data)
        self.assertNotIn(b'name="expiration_date"', response.data)
        data = self.form()
        data.pop('photo')
        data.update(record_key=self.key, revision=record['Updated At'])
        with patch.object(appmod, 'utc_now', return_value=datetime(2027, 1, 1, tzinfo=timezone.utc)):
            response = self.client.post('/ccm/admin/save', data=data)
        self.assertEqual(response.status_code, 200, response.data)
        with appmod.app.app_context():
            edited = appmod.ccm_pending()[self.key]
            self.assertEqual(edited['Expiration Date'], '2027-11-07')
            for field in ('ID Number', 'Created At', 'Verification URL'):
                self.assertEqual(edited[field], record[field])

    def test_sheet_migration_only_appends_expiration_and_rejects_other_changes(self):
        legacy_headers = appmod.CCM_HEADERS[:-1]
        sheet = Mock(col_count=17)
        sheet.row_values.return_value = legacy_headers
        google = Mock()
        google.open.return_value.worksheet.return_value = sheet
        with patch.object(appmod, 'get_gs_client', return_value=google):
            self.assertIs(appmod.ccm_sheet(), sheet)
            sheet.resize.assert_called_once_with(cols=18)
            sheet.update.assert_called_once_with(range_name='A1:R1', values=[appmod.CCM_HEADERS], value_input_option='RAW')
            sheet.row_values.return_value = ['Unexpected'] + legacy_headers[1:]
            with self.assertRaises(appmod.CCMStorageError):
                appmod.ccm_sheet()
            self.assertEqual(sheet.update.call_count, 1)
        row = ['legacy-' + str(i) for i in range(17)]
        legacy = appmod.ccm_sheet_records([legacy_headers, row])[0]
        self.assertEqual(legacy['Verification URL'], row[16])
        self.assertEqual(legacy['Expiration Date'], '')

    def test_backup_round_trip_preserves_expiration_on_append_and_update(self):
        record = self.save()
        for existing in (False, True):
            with self.subTest(existing=existing), appmod.app.app_context():
                # Recreate the local assets after the first backup's confirmed cleanup.
                row = {**record, '_cloud_revision': record['Updated At'] if existing else ''}
                appmod.ccm_write_local_assets(row, {'photo': self.photo,
                    **appmod.ccm_render_id(row, self.photo)})
                appmod.get_db().execute('INSERT OR REPLACE INTO ccm_pending_workers (key,payload) VALUES (?,?)',
                                        (self.key, appmod.json.dumps(row)))
                appmod.get_db().commit()
                values = [list(appmod.CCM_HEADERS)]
                if existing:
                    values.append([record.get(h, '') for h in appmod.CCM_HEADERS])
                sheet = Mock()
                sheet.get_all_values.side_effect = lambda: copy.deepcopy(values)
                sheet.append_row.side_effect = lambda row, **kw: values.append(list(row))
                sheet.update.side_effect = lambda **kw: values.__setitem__(1, list(kw['values'][0]))
                with patch.object(appmod, 'ccm_sheet', return_value=sheet), \
                     patch.object(appmod, 'ccm_upload', side_effect=lambda key, side, body: 'cloud-' + side), \
                     patch.object(appmod, 'ccm_verify_upload'):
                    result = appmod.ccm_backup_worker(self.key)
                self.assertTrue(result['cleaned'])
                self.assertEqual(appmod.ccm_pending(), {})
                cloud = appmod.ccm_sheet_records(values)[0]
                self.assertEqual(cloud['Expiration Date'], '2027-11-07')
                self.assertEqual(cloud['Verification URL'], record['Verification URL'])
                if existing:
                    self.assertEqual(sheet.update.call_args.kwargs['range_name'], 'A2:R2')


class EventRegistrationTests(unittest.TestCase):
    """Public registration and local-authoritative administration use no live services."""

    def setUp(self):
        from PIL import Image
        directory = tempfile.TemporaryDirectory(prefix='district4_event_test_')
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.now = datetime(2026,11,6,8,0,tzinfo=timezone.utc)
        buffer = io.BytesIO()
        Image.new('RGB',(400,400),'#cbdcec').save(buffer,'PNG')
        self.photo = buffer.getvalue()
        self.real_renderer = appmod.event_render_id
        for replacement in (
            patch.object(appmod,'DATABASE',str(self.directory/'app.db')),
            patch.dict(os.environ,DATA_DIR=str(self.directory)),
            patch.dict(appmod.app.config,TESTING=True,SECRET_KEY='event-tests'),
            patch.object(appmod,'utc_now',return_value=self.now),
            patch.object(appmod,'get_gs_client',side_effect=AssertionError('Unexpected live Google request')),
            patch.object(appmod,'event_render_id',return_value=self.photo),
        ):
            replacement.start()
            self.addCleanup(replacement.stop)
        with appmod.app.app_context():
            appmod.init_db()
            appmod.event_tables()
            sheet_cache.sync_cache(appmod.get_db(),appmod.DATABASE,lambda:FakeSheets(),appmod.parse_float,
                                   appmod.parse_sheet_date,lambda:self.now.isoformat())
        self.client = appmod.app.test_client()
        self.client.get('/event-registration')

    def csrf(self):
        with self.client.session_transaction() as state:
            return state['event_csrf']

    def admin(self, developer=False):
        with self.client.session_transaction() as state:
            state['event_admin_until'] = appmod.time.time()+3600
            if developer:
                state.update(ao_logged_in=True,ao_username='Pijeme',ao_role='Area Overseer',ao_area_number='1')

    def create_event(self, kind='District Convention', year=2026):
        self.admin(developer=True)
        response = self.client.post('/event-registration/admin/event/save',data={
            'csrf_token':self.csrf(),'kind':kind,'start_date':f'{year}-12-20','end_date':f'{year}-12-23',
            'theme':'THE URGENT CALL FOR THE GREAT HARVEST','scripture':'Matthew 9:35–38',
            'background':(io.BytesIO(self.photo),'background.png')})
        self.assertEqual(response.status_code,200,response.data)
        with appmod.app.app_context():
            return dict(appmod.event_current())

    def register_details(self,event,name='Attendee One',manual=False):
        self.client.get('/event-registration?new=1')
        with self.client.session_transaction() as state:
            key = state['event_form_key']
        with appmod.app.app_context():
            church = appmod.ccm_churches()[0]
        data = dict(csrf_token=self.csrf(),form_key=key,event_id=str(event['id']),event_key=event['key'],name=name,
                    birthday='2000-06-01',contact='',area=church['area'],church_key=church['key'],
                    address='Forged address',pastor='Forged pastor')
        if manual:
            data.update(new_church='1',church='New Church',address='New Address',pastor='New Pastor')
        response = self.client.post('/event-registration/details',data=data)
        self.assertEqual(response.status_code,200,response.data)
        return key,data

    def capture(self,key):
        response = self.client.post('/event-registration/photo/'+key,data={
            'csrf_token':self.csrf(),'camera_capture':'1','photo':(io.BytesIO(self.photo),'camera-photo.png')})
        self.assertEqual(response.status_code,200,response.data)
        with appmod.app.app_context():
            return dict(appmod.get_db().execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone())

    def test_public_access_admin_password_and_developer_boundary(self):
        page = self.client.get('/event-registration?tab=admin')
        self.assertEqual(page.status_code,200)
        self.assertNotIn(b'EventAdminD4',page.data)
        self.assertNotIn(b'eventCreateButton',page.data)
        self.assertEqual(self.client.post('/event-registration/admin/event/save',data={'csrf_token':self.csrf()}).status_code,403)
        self.assertEqual(self.client.post('/event-registration/admin/login',data={'password':'EventAdminD4'}).status_code,400)
        self.client.post('/event-registration/admin/login',data={'password':'EventAdminD4','csrf_token':self.csrf()})
        self.assertIn(b'eventBackupButton',self.client.get('/event-registration?tab=admin').data)
        self.assertEqual(self.client.post('/event-registration/admin/event/save',data={'csrf_token':self.csrf()}).status_code,403)
        self.admin(developer=True)
        self.assertIn(b'eventCreateButton',self.client.get('/event-registration?tab=admin').data)

    def test_local_details_creation_timestamp_and_registration_number_are_stable(self):
        event = self.create_event()
        key,data = self.register_details(event)
        with appmod.app.app_context():
            original = dict(appmod.get_db().execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone())
            self.assertEqual(original['created_at'],self.now.isoformat())
            self.assertNotEqual(original['address'],'Forged address')
            self.assertNotEqual(original['pastor'],'Forged pastor')
            self.assertEqual(original['church'],'ChurchA')
            self.assertIsNone(original['number'])
        with patch.object(appmod,'utc_now',return_value=datetime(2026,11,7,tzinfo=timezone.utc)):
            self.assertEqual(self.client.post('/event-registration/details',data=data).status_code,200)
            saved = self.capture(key)
            self.capture(key)
        self.assertEqual(saved['created_at'],original['created_at'])
        self.assertEqual(saved['number'],1)
        with appmod.app.app_context():
            self.assertEqual(appmod.event_current()['next_number'],2)
        second,_ = self.register_details(event,'Attendee Two')
        self.assertEqual(self.capture(second)['number'],2)
        new_event = self.create_event('Youth Convention')
        third,_ = self.register_details(new_event,'Attendee Three')
        self.assertEqual(self.capture(third)['number'],1)

    def test_new_church_is_local_and_camera_step_has_no_upload_control(self):
        event = self.create_event()
        key,_ = self.register_details(event,manual=True)
        page = self.client.get('/event-registration/photo/'+key)
        self.assertIn(b'eventCameraCapture',page.data)
        self.assertNotIn(b'type="file"',page.data)
        with appmod.app.app_context():
            attendee = appmod.get_db().execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
            self.assertEqual((attendee['church'],attendee['address'],attendee['pastor']),('New Church','New Address','New Pastor'))
            self.assertIsNone(appmod.get_db().execute("SELECT 1 FROM sheet_accounts_cache WHERE sex='New Church'").fetchone())
        self.assertEqual(self.client.post('/event-registration/photo/'+key,data={
            'csrf_token':self.csrf(),'photo':(io.BytesIO(self.photo),'upload.png')}).status_code,400)
        stranger = appmod.app.test_client()
        self.assertEqual(stranger.get('/event-registration/photo/'+key).status_code,403)
        self.capture(key)
        self.assertEqual(stranger.get('/event-registration/image/'+key+'/photo.png').status_code,403)
        ticket = stranger.get('/event-registration/ticket/'+key)
        self.assertEqual(ticket.status_code,200)
        self.assertNotIn(b'2000-06-01',ticket.data)
        self.assertIn('no-store',ticket.headers['Cache-Control'])

    def test_status_reversal_confirmation_and_print_completion_are_explicit(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        original = self.capture(key)
        url = '/event-registration/admin/status/'+key
        revision = original['revision']
        for field in ('paid','released'):
            data = dict(csrf_token=self.csrf(),field=field,value='1',revision=str(revision))
            response = self.client.post(url,data=data)
            self.assertEqual(response.status_code,200,response.data)
            revision += 1
            data.update(value='0',revision=str(revision))
            self.assertEqual(self.client.post(url,data=data).status_code,400)
            data['confirm'] = 'yes'
            self.assertEqual(self.client.post(url,data=data).status_code,200)
            revision += 1
        page = self.client.get('/event-registration/print/'+key)
        self.assertEqual(page.status_code,200)
        self.assertIn(b'eventPrintComplete',page.data)
        download = self.client.get('/event-registration/image/'+key+'/id.png?download=1')
        self.assertEqual(download.status_code,200)
        download.close()
        with appmod.app.app_context():
            record = appmod.get_db().execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
            self.assertEqual(record['printed'],0)
            self.assertEqual(record['created_at'],original['created_at'])
        response = self.client.post(url,data=dict(csrf_token=self.csrf(),field='printed',value='1',revision=str(revision)))
        self.assertEqual(response.status_code,200)
        self.assertIn(b'event-print-link printed',self.client.get('/event-registration?tab=admin').data)
        self.assertIn(b'class="event-btn printed" id="eventPrintNow"',self.client.get('/event-registration/print/'+key).data)
        self.assertEqual(self.client.post(url,data=dict(csrf_token=self.csrf(),field='paid',value='1',revision='1')).status_code,409)
        response = self.client.post(url,data=dict(csrf_token=self.csrf(),field='printed',value='0',revision=response.json['revision']))
        self.assertEqual(response.status_code,200)
        self.assertNotIn(b'event-print-link printed',self.client.get('/event-registration?tab=admin').data)
        stranger = appmod.app.test_client()
        self.assertEqual(stranger.get('/event-registration/image/'+key+'/id.png').status_code,403)

    def test_id_filename_uses_registration_order_and_keeps_local_portrait(self):
        event = self.create_event()
        key,_ = self.register_details(event,'Juan Dela Cruz')
        attendee = self.capture(key)
        with appmod.app.app_context(), patch.object(appmod,'ccm_upload',return_value='drive-file') as upload, \
             patch.object(appmod,'ccm_verify_upload'):
            self.assertEqual(appmod.event_upload_id(attendee,event),'drive-file')
            self.assertEqual(upload.call_args.kwargs,{'filename':'Event>Juan Dela Cruz>1-ID.png','event_key':event['key']})
            self.assertTrue(appmod.event_asset_path(attendee['photo']).exists())

    def test_registration_stays_local_and_legacy_photo_backup_cannot_upload(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        with patch.object(appmod,'ccm_upload') as upload, patch.object(appmod,'event_sheet') as sheet:
            self.capture(key)
            page = self.client.get('/event-registration/photo/'+key)
            self.assertNotIn(b'eventPhotoBackupStatus',page.data)
            self.assertNotIn(b'eventPhotoBackupRetry',page.data)
            self.assertNotIn(b'Campus Ministry / CCM',page.data)
            legacy = self.client.post('/event-registration/photo/'+key+'/backup',data={'csrf_token':self.csrf()})
            self.assertEqual(legacy.status_code,409)
            self.assertIn('Backup Data',legacy.json['error'])
            upload.assert_not_called()
            sheet.assert_not_called()
        admin = self.client.get('/event-registration?tab=admin')
        self.assertIn(b'>Backup Data</button>',admin.data)
        self.assertNotIn(b'Submit to Google Sheets',admin.data)
        with patch.object(appmod,'sync_from_sheets_if_needed',return_value={}):
            splash = self.client.get('/')
        self.assertNotIn(b'href="/ccm"',splash.data)

    def test_cancel_draft_requires_owner_confirmation_and_preserves_numbering(self):
        event = self.create_event()
        key,details = self.register_details(event)
        url = '/event-registration/cancel/'+key
        page = self.client.get('/event-registration/photo/'+key)
        self.assertIn(b'eventPhotoCancel',page.data)
        self.assertLess(page.data.index(b'Finish registration'),page.data.index(b'Cancel submission'))
        self.assertEqual(self.client.post(url,data={'csrf_token':self.csrf()}).status_code,400)
        stranger = appmod.app.test_client()
        with stranger.session_transaction() as state:
            state.update(event_csrf='stranger',event_admin_until=appmod.time.time()+3600)
        self.assertEqual(stranger.post(url,data={'csrf_token':'stranger','confirm':'yes'}).status_code,403)
        self.assertEqual(self.client.post(url,data={'confirm':'yes'}).status_code,400)
        response = self.client.post(url,data={'csrf_token':self.csrf(),'confirm':'yes'})
        self.assertEqual(response.status_code,200)
        with appmod.app.app_context():
            self.assertIsNone(appmod.get_db().execute('SELECT 1 FROM event_attendees WHERE key=?',(key,)).fetchone())
            self.assertEqual(appmod.event_current()['next_number'],1)
        self.assertEqual(self.client.post('/event-registration/details',data=details).status_code,400)
        next_key,_ = self.register_details(event,'Completed Attendee')
        complete = self.capture(next_key)
        self.assertEqual(complete['number'],1)
        self.assertEqual(self.client.post('/event-registration/cancel/'+next_key,data={
            'csrf_token':self.csrf(),'confirm':'yes'}).status_code,409)
        with appmod.app.app_context():
            self.assertEqual(appmod.get_db().execute('SELECT complete FROM event_attendees WHERE key=?',(next_key,)).fetchone()[0],1)

    def test_backup_saves_only_id_before_sheet_and_refreshes_changed_id(self):
        from PIL import Image
        event = self.create_event()
        key,_ = self.register_details(event,'Juan Dela Cruz')
        attendee = self.capture(key)
        values = [list(appmod.EVENT_HEADERS)]
        sheet = Mock()
        calls = []
        sheet.get_all_values.side_effect = lambda: copy.deepcopy(values)
        def append(row,**kwargs):
            calls.append('sheet')
            values.append(list(row))
        sheet.append_row.side_effect = append
        sheet.update.side_effect = lambda **kw: values.__setitem__(int(kw['range_name'].split(':')[0][1:])-1,list(kw['values'][0]))
        def upload(record,side,body,**kwargs):
            calls.append(side)
            return 'drive-'+side
        changed = io.BytesIO()
        Image.new('RGB',(400,400),'gold').save(changed,'PNG')
        with appmod.app.app_context(), patch.object(appmod,'ccm_upload',side_effect=upload) as uploads, \
             patch.object(appmod,'ccm_verify_upload') as verify, \
             patch.object(appmod,'event_sheet',return_value=(sheet,appmod.EVENT_HEADERS)), \
             patch.object(appmod,'event_render_id',return_value=self.photo) as render:
            self.assertTrue(appmod.event_backup_attendee(key)['clean'])
            self.assertEqual(calls,['id','sheet'])
            self.assertEqual(uploads.call_args.kwargs,{'filename':'Event>Juan Dela Cruz>1-ID.png','event_key':event['key']})
            verify.assert_any_call('drive-id',self.photo)
            stored = dict(appmod.get_db().execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone())
            self.assertEqual((stored['photo_drive_id'],stored['id_drive_id']),('','drive-id'))
            self.assertTrue(stored['id_backup_hash'])
            uploads.reset_mock()
            appmod.event_backup_attendee(key)
            uploads.assert_not_called()
            appmod.get_db().execute("UPDATE registration_events SET theme='New theme' WHERE id=?",(event['id'],))
            appmod.get_db().commit()
            render.return_value = changed.getvalue()
            appmod.event_backup_attendee(key)
            self.assertEqual(uploads.call_count,1)
            self.assertEqual(uploads.call_args.args[1],'id')
            self.assertEqual(render.call_args.args[0]['theme'],'New theme')
            self.assertTrue(appmod.event_asset_path(attendee['photo']).exists())
            self.assertEqual(len(values),2)

    def test_id_backup_failure_blocks_sheet_write_and_retry_never_uploads_portrait(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        attendee = self.capture(key)
        values = [list(appmod.EVENT_HEADERS)]
        sheet = Mock()
        sheet.get_all_values.side_effect = lambda: copy.deepcopy(values)
        sheet.append_row.side_effect = lambda row,**kwargs: values.append(list(row))
        attempts = []
        def upload(record,side,body,**kwargs):
            attempts.append(side)
            if attempts == ['id']:
                raise appmod.CCMStorageError('ID storage unavailable')
            return 'drive-'+side
        with patch.object(appmod,'ccm_upload',side_effect=upload), patch.object(appmod,'ccm_verify_upload'), \
             patch.object(appmod,'event_sheet',return_value=(sheet,appmod.EVENT_HEADERS)) as connect:
            url = '/event-registration/admin/backup/'+key
            response = self.client.post(url,data={'csrf_token':self.csrf()})
            self.assertEqual(response.status_code,503)
            connect.assert_not_called()
            with appmod.app.app_context():
                stored = appmod.get_db().execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
                self.assertEqual(stored['photo_drive_id'],'')
                self.assertEqual(stored['id_drive_id'],'')
                self.assertEqual(stored['backed_up_revision'],0)
                self.assertTrue(appmod.event_asset_path(attendee['photo']).is_file())
            self.assertEqual(self.client.post(url,data={'csrf_token':self.csrf()}).status_code,200)
            self.assertEqual(attempts,['id','id'])
            self.assertEqual(len(values),2)

    def test_event_design_changed_during_backup_is_reported_for_retry(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        self.capture(key)
        values = [list(appmod.EVENT_HEADERS)]
        sheet = Mock()
        sheet.get_all_values.side_effect = lambda: copy.deepcopy(values)
        def append(row,**kwargs):
            values.append(list(row))
            appmod.get_db().execute("UPDATE registration_events SET theme='Changed during backup' WHERE id=?",(event['id'],))
            appmod.get_db().commit()
        sheet.append_row.side_effect = append
        with appmod.app.app_context(), patch.object(appmod,'event_sheet',return_value=(sheet,appmod.EVENT_HEADERS)), \
             patch.object(appmod,'event_upload_id',return_value='drive-id'):
            self.assertFalse(appmod.event_backup_attendee(key)['clean'])

    def test_google_submission_overwrites_remote_values_and_is_idempotent(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        original = self.capture(key)
        headers = list(reversed(appmod.EVENT_HEADERS))  # Export follows names, not fixed header order.
        values = [headers]
        sheet = Mock()
        sheet.get_all_values.side_effect = lambda: copy.deepcopy(values)
        sheet.append_row.side_effect = lambda row,**kw: values.append(list(row))
        sheet.update.side_effect = lambda **kw: values.__setitem__(int(kw['range_name'].split(':')[0][1:])-1,list(kw['values'][0]))
        with appmod.app.app_context(), patch.object(appmod,'event_sheet',return_value=(sheet,headers)), \
             patch.object(appmod,'event_upload_id',return_value='drive-id'):
            appmod.event_backup_attendee(key)
            values[1][headers.index('Paid')] = 'Remote value'
            values[1][headers.index('Name')] = 'Edited remotely'
            appmod.event_backup_attendee(key)
            self.assertEqual(len(values),2)
            self.assertEqual(values[1][headers.index('Paid')],'')
            self.assertEqual(values[1][headers.index('Name')],original['name'])
            self.assertEqual(values[1][headers.index('Date created')],original['created_at'])
            self.assertEqual(values[1][headers.index('Event')],event['name'])
            self.assertTrue(appmod.event_asset_path(original['photo']).exists())
            def uncertain_append(row,**kw):
                values.append(list(row))
                raise TimeoutError('Acknowledgement lost')
            values[:] = [headers]
            appmod.get_db().execute("UPDATE event_attendees SET sheet_row=NULL,cloud_snapshot='' WHERE key=?",(key,))
            appmod.get_db().commit()
            sheet.append_row.side_effect = uncertain_append
            with self.assertRaises(TimeoutError):
                appmod.event_backup_attendee(key)
            appmod.event_backup_attendee(key)
            self.assertEqual(len(values),2)

    def test_drive_bridge_preserves_ccm_uploads_and_requires_event_filename_acknowledgement(self):
        filename = 'Event>Juan Dela Cruz>10.png'
        response = Mock()
        response.json.return_value = {'ok':True,'file_id':'drive-file'}
        with patch.dict(os.environ,CCM_DRIVE_SCRIPT_URL='https://script.google.com/macros/s/test/exec',CCM_DRIVE_API_TOKEN='x'*32,
                        EVENT_DRIVE_SCRIPT_URL='https://script.google.com/macros/s/event-test/exec',EVENT_DRIVE_API_TOKEN='y'*32), \
             patch.object(appmod.requests,'post',return_value=response) as post:
            self.assertEqual(appmod.ccm_upload('a'*32,'photo',self.photo),'drive-file')
            self.assertNotIn('purpose',post.call_args.kwargs['json'])
            self.assertEqual(post.call_args.args[0],os.environ['CCM_DRIVE_SCRIPT_URL'])
            self.assertEqual(post.call_args.kwargs['json']['token'],'x'*32)
            with self.assertRaisesRegex(appmod.CCMStorageError,'not confirmed in the correct folder'):
                appmod.ccm_upload('a'*32,'photo',self.photo,filename=filename,event_key='b'*32)
            response.json.return_value = {'ok':True,'file_id':'drive-file','name':filename,
                                          'purpose':'event','folder_id':appmod.EVENT_FOLDER_ID,'side':'photo'}
            self.assertEqual(appmod.ccm_upload('a'*32,'photo',self.photo,filename=filename,event_key='b'*32),'drive-file')
            payload = post.call_args.kwargs['json']
            self.assertEqual((payload['purpose'],payload['filename'],payload['event_key']),('event',filename,'b'*32))
            self.assertEqual(post.call_args.args[0],os.environ['EVENT_DRIVE_SCRIPT_URL'])
            self.assertEqual(payload['token'],'y'*32)

    def test_event_upload_requires_separate_connection_without_ccm_fallback(self):
        with patch.dict(os.environ,CCM_DRIVE_SCRIPT_URL='https://script.google.com/macros/s/test/exec',CCM_DRIVE_API_TOKEN='x'*32,
                        EVENT_DRIVE_SCRIPT_URL='',EVENT_DRIVE_API_TOKEN=''), \
             patch.object(appmod.requests,'post') as post, patch.object(appmod,'ccm_drive') as direct:
            with self.assertRaisesRegex(appmod.CCMStorageError,'EVENT_DRIVE_SCRIPT_URL and EVENT_DRIVE_API_TOKEN'):
                appmod.ccm_upload('a'*32,'photo',self.photo,filename='Event>Sample>1.png',event_key='b'*32)
            post.assert_not_called()
            direct.assert_not_called()

    def test_event_upload_validates_folder_side_and_id_acknowledgement(self):
        filename = 'Event>Sample>1-ID.png'
        result = {'ok':True,'file_id':'event-id-file','name':filename,'purpose':'event',
                  'folder_id':appmod.EVENT_FOLDER_ID,'side':'id'}
        response = Mock()
        with patch.dict(os.environ,EVENT_DRIVE_SCRIPT_URL='https://script.google.com/macros/s/event-test/exec',EVENT_DRIVE_API_TOKEN='y'*32), \
             patch.object(appmod.requests,'post',return_value=response) as post:
            for change in [{'folder_id':appmod.CCM_FOLDER_ID},{'side':'photo'},{'purpose':'ccm'},
                           {'name':'CCM-wrong.png'}]:
                with self.subTest(change=change):
                    response.json.return_value = {**result,**change}
                    with self.assertRaisesRegex(appmod.CCMStorageError,'not confirmed in the correct folder'):
                        appmod.ccm_upload('a'*32,'id',self.photo,filename=filename,event_key='b'*32)
            response.json.return_value = result
            self.assertEqual(appmod.ccm_upload('a'*32,'id',self.photo,filename=filename,event_key='b'*32),'event-id-file')
            self.assertEqual(post.call_args.kwargs['json']['side'],'id')
            response.json.return_value = {'ok':False,'code':'CCM_ONLY'}
            with self.assertRaisesRegex(appmod.CCMStorageError,'CCM-only upload endpoint'):
                appmod.ccm_upload('a'*32,'id',self.photo,filename=filename,event_key='b'*32)

    def test_partial_drive_bridge_configuration_never_falls_back_to_service_account(self):
        with patch.dict(os.environ,CCM_DRIVE_SCRIPT_URL='',CCM_DRIVE_API_TOKEN='x'*32), \
             patch.object(appmod,'ccm_drive') as direct, patch.object(appmod.requests,'post') as post:
            with self.assertRaisesRegex(appmod.CCMStorageError,'CCM_DRIVE_SCRIPT_URL is missing'):
                appmod.ccm_upload('a'*32,'photo',self.photo)
            direct.assert_not_called()
            post.assert_not_called()

    def test_drive_bridge_returns_actionable_script_failures_without_fallback(self):
        response = Mock()
        with patch.dict(os.environ,CCM_DRIVE_SCRIPT_URL='https://script.google.com/macros/s/test/exec',CCM_DRIVE_API_TOKEN='x'*32), \
             patch.object(appmod.requests,'post',return_value=response), patch.object(appmod,'ccm_drive') as direct:
            for code,message in [('AUTH','rejected its API token'),('INPUT','latest deployment/ccm_drive.gs'),
                                 ('IMAGE','rejected this PNG'),('DRIVE','Apps Script Executions log')]:
                with self.subTest(code=code):
                    response.json.return_value = {'ok':False,'code':code}
                    with self.assertRaisesRegex(appmod.CCMStorageError,message):
                        appmod.ccm_upload('a'*32,'photo',self.photo)
            direct.assert_not_called()

    def test_service_account_storage_quota_error_identifies_deployment_settings(self):
        response = Mock(ok=False,status_code=403)
        response.json.return_value = {'error':{'errors':[{'reason':'storageQuotaExceeded'}]}}
        with patch.object(appmod,'google_credentials'), patch.object(appmod.requests,'request',return_value=response):
            with self.assertRaisesRegex(appmod.CCMStorageError,'CCM_DRIVE_SCRIPT_URL and CCM_DRIVE_API_TOKEN'):
                appmod.ccm_drive('POST','upload/drive/v3/files')

    def test_status_changes_during_backup_remain_unsynced_and_failure_keeps_local_data(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        original = self.capture(key)
        values = [list(appmod.EVENT_HEADERS)]
        sheet = Mock()
        sheet.get_all_values.side_effect = lambda: copy.deepcopy(values)
        def append_and_change(row,**kw):
            values.append(list(row))
            appmod.get_db().execute('UPDATE event_attendees SET paid=1,revision=revision+1 WHERE key=?',(key,))
            appmod.get_db().commit()
        sheet.append_row.side_effect = append_and_change
        with appmod.app.app_context(), patch.object(appmod,'event_sheet',return_value=(sheet,appmod.EVENT_HEADERS)), \
             patch.object(appmod,'event_upload_id',return_value='drive-id'):
            result = appmod.event_backup_attendee(key)
            self.assertFalse(result['clean'])
            attendee = appmod.get_db().execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
            self.assertEqual(attendee['paid'],1)
            self.assertLess(attendee['backed_up_revision'],attendee['revision'])
        with patch.object(appmod,'event_upload_id',side_effect=appmod.CCMStorageError('Drive unavailable')):
            response = self.client.post('/event-registration/admin/backup/'+key,data={'csrf_token':self.csrf()})
            self.assertEqual(response.status_code,503)
        with appmod.app.app_context():
            self.assertTrue(appmod.event_asset_path(original['photo']).exists())
            self.assertEqual(appmod.get_db().execute('SELECT COUNT(*) FROM event_attendees').fetchone()[0],1)

    def test_event_change_keeps_old_details_and_invalidates_old_camera_step(self):
        first = self.create_event()
        key,_ = self.register_details(first)
        second = self.create_event('Youth Convention')
        response = self.client.post('/event-registration/photo/'+key,data={
            'csrf_token':self.csrf(),'camera_capture':'1','photo':(io.BytesIO(self.photo),'camera.png')})
        self.assertEqual(response.status_code,400)
        self.client.get('/event-registration')
        with self.client.session_transaction() as state:
            self.assertNotEqual(state['event_form_key'],key)
            self.assertEqual(state['event_form_event_id'],second['id'])
        with appmod.app.app_context():
            self.assertEqual(appmod.get_db().execute('SELECT complete FROM event_attendees WHERE key=?',(key,)).fetchone()[0],0)

    def test_admin_table_hides_sheet_only_fields_and_deletion_revokes_registration(self):
        event = self.create_event()
        key,_ = self.register_details(event,'Wrong Registration')
        original = self.capture(key)
        page = self.client.get('/event-registration?tab=admin')
        headings = page.data.split(b'<table class="event-table">')[1].split(b'<thead>')[1].split(b'</thead>')[0]
        for column in ('Bday','Contact #','Pastor','Date created','Event','Google copy'):
            self.assertNotIn(('<th>'+column+'</th>').encode(),headings)
        self.assertIn(b'<th>Delete</th>',headings)
        body = page.data.split(b'id="eventAttendeeRows"')[1].split(b'</tbody>')[0]
        self.assertNotIn(b'2000-06-01',body)
        self.assertNotIn(b'Pastor One',body)
        self.assertIn(b'data-delete-url=',body)
        url = '/event-registration/admin/delete/'+key
        self.assertEqual(self.client.post(url,data={'csrf_token':self.csrf(),'revision':original['revision']}).status_code,400)
        self.assertEqual(appmod.app.test_client().post(url,data={'confirm':'yes'}).status_code,403)
        data = dict(csrf_token=self.csrf(),confirm='yes',revision=0)
        self.assertEqual(self.client.post(url,data=data).status_code,409)
        data['revision'] = original['revision']
        self.assertEqual(self.client.post(url,data=data).status_code,200)
        self.assertEqual(self.client.post(url,data=data).status_code,200)  # Repeated acknowledgement is safe.
        self.assertNotIn(b'Wrong Registration',self.client.get('/event-registration?tab=admin').data)
        for path in ('/event-registration/ticket/','/event-registration/print/','/event-registration/photo/'):
            self.assertEqual(self.client.get(path+key).status_code,404)
        second,_ = self.register_details(event,'Correct Registration')
        self.assertEqual(self.capture(second)['number'],2)
        with appmod.app.app_context():
            stored = appmod.get_db().execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
            self.assertEqual(stored['birthday'],original['birthday'])
            self.assertEqual(stored['created_at'],original['created_at'])

    def test_only_developer_can_delete_event_and_name_can_be_reused(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        self.capture(key)
        ordinary_admin = appmod.app.test_client()
        with ordinary_admin.session_transaction() as state:
            state.update(event_admin_until=appmod.time.time()+3600,event_csrf='ordinary-admin')
        url = f"/event-registration/admin/event/{event['id']}/delete"
        self.assertNotIn(b'eventDeleteEventButton',ordinary_admin.get('/event-registration?tab=admin').data)
        self.assertEqual(ordinary_admin.post(url,data={'csrf_token':'ordinary-admin','confirm':'yes','updated_at':event['updated_at'],'password':'EventAdminD4'}).status_code,403)
        self.assertIn(b'eventDeleteEventButton',self.client.get('/event-registration?tab=admin').data)
        data = dict(csrf_token=self.csrf(),updated_at=event['updated_at'])
        self.assertEqual(self.client.post(url,data=data).status_code,400)
        data.update(confirm='yes')
        self.assertEqual(self.client.post(url,data=data).status_code,403)
        data['password'] = 'incorrect'
        self.assertEqual(self.client.post(url,data=data).status_code,403)
        with appmod.app.app_context():
            self.assertEqual(appmod.event_current()['id'],event['id'])
            self.assertEqual(appmod.get_db().execute('SELECT deleted_at FROM event_attendees WHERE key=?',(key,)).fetchone()[0],'')
        data.update(password='EventAdminD4',updated_at='stale')
        self.assertEqual(self.client.post(url,data=data).status_code,409)
        data['updated_at'] = event['updated_at']
        self.assertEqual(self.client.post(url,data=data).status_code,200)
        with appmod.app.app_context():
            self.assertIsNone(appmod.event_current())
        self.assertEqual(self.client.get('/event-registration/ticket/'+key).status_code,404)
        self.assertEqual(self.client.get(f"/event-registration/background/{event['id']}.png").status_code,404)
        self.assertEqual(self.client.post(url,data={k:v for k,v in data.items() if k!='password'}).status_code,403)
        queue = self.client.post('/event-registration/admin/backup',data={'csrf_token':self.csrf()})
        self.assertEqual(queue.json['keys'],[key])
        replacement = self.create_event()
        self.assertEqual(replacement['name'],event['name'])
        self.assertNotEqual(replacement['id'],event['id'])
        self.assertEqual(replacement['next_number'],1)
        self.client.get('/event-registration')
        with self.client.session_transaction() as state:
            old_form_key = state['event_form_key']
        response = self.client.post(f"/event-registration/admin/event/{replacement['id']}/delete",data={
            'csrf_token':self.csrf(),'confirm':'yes','updated_at':replacement['updated_at'],'password':'EventAdminD4'})
        self.assertEqual(response.status_code,200)
        next_event = self.create_event(year=2027)
        self.assertEqual(self.client.post('/event-registration/details',data={
            'csrf_token':self.csrf(),'event_id':replacement['id'],'event_key':replacement['key'],'form_key':old_form_key}).status_code,409)
        self.client.get('/event-registration')
        with self.client.session_transaction() as state:
            self.assertEqual(state['event_form_event_key'],next_event['key'])
            self.assertNotEqual(state['event_form_key'],old_form_key)

    def test_sheet_deletion_removes_correct_row_and_retries_lost_acknowledgement(self):
        event = self.create_event()
        key,_ = self.register_details(event,'Delete Me')
        original = self.capture(key)
        headers = list(appmod.EVENT_HEADERS)
        values = [headers]
        sheet = Mock()
        sheet.get_all_values.side_effect = lambda: copy.deepcopy(values)
        sheet.append_row.side_effect = lambda row,**kw: values.append(list(row))
        sheet.delete_rows.side_effect = lambda number: values.pop(number-1)
        with appmod.app.app_context(), patch.object(appmod,'event_sheet',return_value=(sheet,headers)), \
             patch.object(appmod,'event_upload_id',return_value='drive-id'):
            appmod.event_backup_attendee(key)
        self.assertEqual(self.client.post('/event-registration/admin/delete/'+key,data={
            'csrf_token':self.csrf(),'confirm':'yes','revision':original['revision']}).status_code,200)
        original_cloud_row = list(values[1])
        for field in ('Name','Bday','Date created'):
            values[1][headers.index(field)] = 'Changed externally'
        with appmod.app.app_context(), patch.object(appmod,'event_sheet',return_value=(sheet,headers)):
            with self.assertRaisesRegex(appmod.CCMStorageError,'cannot be identified safely'):
                appmod.event_backup_attendee(key)
            self.assertIsNotNone(appmod.get_db().execute('SELECT 1 FROM event_attendees WHERE key=?',(key,)).fetchone())
            sheet.delete_rows.assert_not_called()
        values[1] = original_cloud_row
        def uncertain_delete(number):
            values.pop(number-1)
            raise TimeoutError('Acknowledgement lost')
        sheet.delete_rows.side_effect = uncertain_delete
        with appmod.app.app_context(), patch.object(appmod,'event_sheet',return_value=(sheet,headers)), \
             patch.object(appmod,'event_upload_id') as upload:
            with self.assertRaises(TimeoutError):
                appmod.event_backup_attendee(key)
            self.assertIsNotNone(appmod.get_db().execute('SELECT 1 FROM event_attendees WHERE key=?',(key,)).fetchone())
            result = appmod.event_backup_attendee(key)
            self.assertTrue(result['removed'])
            self.assertEqual(values,[headers])
            self.assertIsNone(appmod.get_db().execute('SELECT 1 FROM event_attendees WHERE key=?',(key,)).fetchone())
            upload.assert_not_called()
            sheet.delete_rows.assert_called_once_with(2)

    def test_deletion_schema_migration_preserves_existing_registrations(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        original = self.capture(key)
        with appmod.app.app_context():
            db = appmod.get_db()
            for table in ('event_attendees','registration_events'):
                db.execute(f'ALTER TABLE {table} DROP COLUMN deleted_at')
            for column in ('id_drive_id','id_backup_hash'):
                db.execute(f'ALTER TABLE event_attendees DROP COLUMN {column}')
            db.commit()
            appmod.event_tables()
            stored = db.execute('SELECT * FROM event_attendees WHERE key=?',(key,)).fetchone()
            self.assertEqual(stored['deleted_at'],'')
            self.assertEqual((stored['id_drive_id'],stored['id_backup_hash']),('',''))
            for field in ('name','birthday','contact','pastor','created_at','number'):
                self.assertEqual(stored[field],original[field])
            self.assertEqual(appmod.event_current()['id'],event['id'])
        with self.client.session_transaction() as state:
            state.pop('event_form_event_key',None)
        page = self.client.get('/event-registration')
        self.assertIn(b'View confirmation',page.data)
        with self.client.session_transaction() as state:
            self.assertEqual(state['event_registration_key'],key)

    def test_pastors_and_area_overseers_receive_one_ptr_prefix(self):
        with appmod.app.app_context():
            appmod.get_db().execute("UPDATE sheet_accounts_cache SET name='Juan Dela Cruz' WHERE username='pastorone'")
            appmod.get_db().commit()
            cases = [
                ('Juan Dela Cruz','ChurchA',False,'Ptr. Juan Dela Cruz'),
                ('Developer','DevChurch',False,'Ptr. Developer'),
                ('Developer','ChurchA',False,'Developer'),
                ('Attendee One','ChurchA',False,'Attendee One'),
                ('Ana Santos','New Church',True,'Ptr. Ana Santos'),
                ('Ptr. Ptr. Juan Dela Cruz','ChurchA',True,'Ptr. Juan Dela Cruz'),
                ('Ptr.Juan Dela Cruz','ChurchA',True,'Ptr. Juan Dela Cruz'),
            ]
            for name,church,minister,expected in cases:
                with self.subTest(name=name,church=church):
                    self.assertEqual(appmod.event_registration_name(name,'1',church,minister),expected)
            with self.assertRaises(ValueError):
                appmod.event_registration_name('Ptr.','1','ChurchA',True)
        event = self.create_event()
        key,data = self.register_details(event,'Ana Santos',manual=True)
        data['minister'] = '1'
        self.assertEqual(self.client.post('/event-registration/details',data=data).status_code,200)
        attendee = self.capture(key)
        self.assertEqual(attendee['name'],'Ptr. Ana Santos')
        confirmation = self.client.get('/event-registration/photo/'+key)
        self.assertIn(b'Ptr. Ana Santos',confirmation.data)
        page = self.client.get('/event-registration?new=1')
        self.assertIn(b'name="minister"',page.data)

    def test_id_layout_fits_long_theme_and_rejects_unreadable_overflow(self):
        from PIL import Image
        event = self.create_event()
        image = self.real_renderer(event,background=self.photo)
        long_event = {**event,'theme':'Together we answer the urgent call to serve our communities with faith, hope, love and a commitment to the great harvest'}
        long_image = self.real_renderer(long_event,background=self.photo)
        with Image.open(io.BytesIO(image)) as first, Image.open(io.BytesIO(long_image)) as second:
            self.assertEqual(first.size,(1000,1500))
            self.assertEqual(first.crop((300,648,700,1043)).tobytes(),second.crop((300,648,700,1043)).tobytes())
            self.assertEqual(first.crop((400,1290,600,1490)).tobytes(),second.crop((400,1290,600,1490)).tobytes())
        with self.assertRaisesRegex(ValueError,'too long'):
            self.real_renderer({**event,'theme':'Too much text '*45},background=self.photo)

    def meal_api(self,event,action,payload=None):
        url = '/event-registration/api/v1/events/'+event['key']+'/'+action
        headers = {'Authorization':'Bearer '+'m'*64}
        return self.client.get(url,headers=headers) if payload is None else self.client.post(url,json=payload,headers=headers)

    def scanner_enroll(self,device='11111111-1111-1111-1111-111111111111'):
        response = self.client.post('/api/v2/meal-scanner/enroll',json={
            'device_id':device,'label':'Test master','admin_password':'EventAdminD4'})
        self.assertEqual(response.status_code,200,response.json)
        return {'X-IOWO-Device-ID':device,'Authorization':'Bearer '+response.json['access_token']}

    def test_scanner_requests_bypass_google_and_logging_and_return_private_json(self):
        with patch.object(appmod,'sync_from_sheets_if_needed',side_effect=AssertionError('Scanner must not sync Google')), \
             patch.object(appmod,'_log_visit_if_needed') as visits:
            prefix = '/api/v2/meal-scanner'
            for method,path,status in [('get','/events',401),('get','/missing',404),('get','',404),('post','/events',405)]:
                response = getattr(self.client,method)(prefix+path)
                self.assertEqual(response.status_code,status)
                self.assertTrue(response.is_json)
                self.assertIn('no-store',response.headers['Cache-Control'])
                self.assertEqual(response.headers['X-Content-Type-Options'],'nosniff')
            headers = self.scanner_enroll()
            self.assertEqual(self.client.get(prefix+'/events',headers=headers).status_code,200)
            visits.assert_not_called()

    def test_scanner_roster_filters_paid_complete_deleted_and_event_scope(self):
        event = self.create_event()
        paid,_ = self.register_details(event,'Paid delegate');self.capture(paid)
        unpaid,_ = self.register_details(event,'Unpaid delegate');self.capture(unpaid)
        deleted,_ = self.register_details(event,'Deleted delegate');self.capture(deleted)
        draft,_ = self.register_details(event,'Unfinished delegate')
        foreign = self.create_event('Youth Convention')
        other,_ = self.register_details(foreign,'Other event');self.capture(other)
        with appmod.app.app_context():
            db = appmod.get_db()
            db.execute('UPDATE event_attendees SET paid=1 WHERE key<>?',(unpaid,))
            db.execute('UPDATE event_attendees SET deleted_at=? WHERE key=?',('test-deleted',deleted))
            db.commit()
            original = dict(db.execute('SELECT * FROM event_attendees WHERE key=?',(paid,)).fetchone())
        with patch.object(appmod,'sync_from_sheets_if_needed',side_effect=AssertionError('Unexpected Google sync')):
            headers = self.scanner_enroll()
            url = '/api/v2/meal-scanner/events/'+event['key']+'/paid-registrations'
            response = self.client.get(url,headers=headers)
            self.assertEqual(response.status_code,200,response.json)
            data = response.json
            self.assertEqual([p['key'] for p in data['attendees']],[paid])
            self.assertEqual(data['total'],1)
            self.assertFalse(original['printed'])
            self.assertFalse(original['released'])
            for field in ('birthday','contact','pastor','address','photo','id_drive_id'):
                self.assertNotIn(field,data['attendees'][0])
            unchanged = self.client.get(url+'?cursor='+data['cursor'],headers=headers).json
            self.assertFalse(unchanged['changed'])
            self.assertEqual(unchanged['attendees'],[])
            with appmod.app.app_context():
                appmod.get_db().execute('UPDATE event_attendees SET paid=0 WHERE key=?',(paid,));appmod.get_db().commit()
            removed = self.client.get(url+'?cursor='+data['cursor'],headers=headers).json
            self.assertTrue(removed['changed'])
            self.assertEqual(removed['attendees'],[])
            self.assertEqual(removed['total'],0)
        with appmod.app.app_context():
            self.assertEqual(appmod.get_db().execute('SELECT COUNT(*) FROM event_attendees').fetchone()[0],5)
            tables = {row[0] for row in appmod.get_db().execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn('event_meal_claims',tables)
            self.assertNotIn('event_meal_audit',tables)

    def test_scanner_deleted_unknown_events_and_ordinary_html_errors(self):
        event = self.create_event()
        headers = self.scanner_enroll()
        url = '/api/v2/meal-scanner/events/'+event['key']+'/paid-registrations'
        with appmod.app.app_context():
            appmod.get_db().execute('UPDATE registration_events SET deleted_at=?,active=0 WHERE id=?',('test-deleted',event['id']))
            appmod.get_db().commit()
        listing = self.client.get('/api/v2/meal-scanner/events',headers=headers).json
        self.assertEqual(listing['events'],[])
        self.assertEqual(listing['deleted_event_keys'],[event['key']])
        response = self.client.get(url,headers=headers)
        self.assertEqual(response.status_code,410)
        self.assertEqual(response.json['code'],'EVENT_DELETED')
        with appmod.app.app_context():
            appmod.get_db().execute('DELETE FROM registration_events WHERE id=?',(event['id'],));appmod.get_db().commit()
        response = self.client.get(url,headers=headers)
        self.assertEqual(response.status_code,404)
        self.assertEqual(response.json['code'],'EVENT_NOT_FOUND')
        with patch.object(appmod,'sync_from_sheets_if_needed',return_value={}):
            response = self.client.get('/test-unknown-normal-website-page')
        self.assertEqual(response.status_code,404)
        self.assertFalse(response.is_json)

    def test_scanner_revocation_and_enrollment_password_attempts_are_rate_limited(self):
        prefix = '/api/v2/meal-scanner'
        device = '11111111-1111-1111-1111-111111111111'
        for operation in ('enroll','revoke'):
            for _ in range(5):
                response = self.client.post(prefix+'/'+operation,json={'device_id':device,'admin_password':'incorrect'})
                self.assertEqual(response.status_code,403,response.json)
            response = self.client.post(prefix+'/'+operation,json={'device_id':device,'admin_password':'EventAdminD4'})
            self.assertEqual(response.status_code,429)
            self.assertEqual(response.json['code'],'RATE_LIMITED')

    def test_scanner_device_token_rotation_revocation_and_hash_storage(self):
        prefix = '/api/v2/meal-scanner'
        first = self.scanner_enroll()
        second = self.scanner_enroll()
        self.assertNotEqual(first['Authorization'],second['Authorization'])
        self.assertEqual(self.client.get(prefix+'/events',headers=first).status_code,401)
        self.assertEqual(self.client.get(prefix+'/events',headers=second).status_code,200)
        with appmod.app.app_context():
            row = appmod.get_db().execute('SELECT * FROM meal_scanner_download_devices').fetchone()
            token = second['Authorization'][7:]
            self.assertNotEqual(row['secret_hash'],token)
            import hashlib
            self.assertEqual(row['secret_hash'],hashlib.sha256(token.encode()).hexdigest())
        response = self.client.post(prefix+'/revoke',json={'device_id':second['X-IOWO-Device-ID'],'admin_password':'EventAdminD4'})
        self.assertEqual(response.status_code,200)
        response = self.client.get(prefix+'/events',headers=second)
        self.assertEqual(response.status_code,401)
        self.assertEqual(response.json['code'],'REVOKED_OR_EXPIRED')

    def test_scanner_malformed_json_input_and_size_limit(self):
        prefix = '/api/v2/meal-scanner'
        for operation in ('enroll','revoke'):
            for payload in ([],[1,2],'not an object'):
                response = self.client.post(prefix+'/'+operation,json=payload)
                self.assertEqual(response.status_code,400)
                self.assertEqual(response.json['code'],'INVALID_JSON')
            response = self.client.post(prefix+'/'+operation,json={'device_id':123,'admin_password':[]})
            self.assertEqual(response.status_code,400)
            self.assertEqual(response.json['code'],'INVALID_INPUT')
            response = self.client.post(prefix+'/'+operation,data='x'*(16*1024+1),content_type='application/json')
            self.assertEqual(response.status_code,413)
            self.assertEqual(response.json['code'],'REQUEST_TOO_LARGE')

    def meal_set(self,event,revision=1,enabled=True):
        return self.meal_api(event,'meal-sets',{'batch_id':'sets-'+str(revision),'sets':[{
            'set_id':'convention-meals','name':'App configured meals','revision':revision,
            'sessions':[{'session_id':'opening-dinner','name':'Opening Dinner','date':'2026-12-20','enabled':enabled}]}]})

    def meal_claim(self,ticket,claim='claim-1'):
        return dict(claim_id=claim,ticket_id=ticket,session_id='opening-dinner',scanner_id='slave-1',served_at='2026-12-20T18:00:00+08:00')

    def test_master_auth_roster_snapshots_and_private_full_details(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        self.capture(key)
        prefix = '/event-registration/api/v1/events'
        with patch.dict(os.environ,EVENT_MEAL_MASTER_TOKEN=''):
            self.assertEqual(self.client.get(prefix).status_code,503)
        with patch.dict(os.environ,EVENT_MEAL_MASTER_TOKEN='m'*64):
            self.assertEqual(self.client.get(prefix).status_code,401)  # Admin cookie is not master auth.
            self.assertEqual(self.client.get(prefix,headers={'Authorization':'Bearer wrong'}).status_code,401)
            roster = self.meal_api(event,'delegates')
            self.assertEqual(roster.status_code,200)
            item = roster.json['delegates'][0]
            self.assertEqual(item['ticket_id'],key)
            self.assertFalse(item['meal_eligible'])
            self.assertNotIn('birthday',item)
            self.assertNotIn('contact',item)
            self.assertIn('no-store',roster.headers['Cache-Control'])
            cursor = roster.json['snapshot']
            self.assertFalse(self.meal_api(event,'changes?cursor='+cursor).json['changed'])
            with appmod.app.app_context():
                appmod.get_db().execute('UPDATE event_attendees SET paid=1 WHERE key=?',(key,));appmod.get_db().commit()
            updated = self.meal_api(event,'changes?cursor='+cursor).json
            self.assertTrue(updated['changed'])
            self.assertTrue(updated['delegates'][0]['meal_eligible'])
            self.assertFalse(updated['delegates'][0]['processed'])
            self.assertEqual(updated['mode'],'replace_event_snapshot')
            private = self.client.get('/event-registration/admin/delegate/'+key)
            self.assertEqual(private.json['delegate']['birthday'],'2000-06-01')
            self.assertEqual(appmod.app.test_client().get('/event-registration/admin/delegate/'+key).status_code,403)
            self.client.post('/event-registration/admin/delete/'+key,data={'csrf_token':self.csrf(),'confirm':'yes','revision':1})
            self.assertEqual(self.meal_api(event,'delegates').json['delegates'],[])

    def test_meal_sets_claim_retry_duplicates_and_audit_void(self):
        event = self.create_event()
        key,_ = self.register_details(event)
        self.capture(key)
        with patch.dict(os.environ,EVENT_MEAL_MASTER_TOKEN='m'*64):
            self.assertEqual(self.meal_set(event).status_code,200)
            claim = self.meal_claim(key)
            denied = self.meal_api(event,'claims',{'batch_id':'unpaid','claims':[dict(claim,claim_id='unpaid-claim')]})
            self.assertEqual(denied.json['results'][0]['status'],'unpaid')
            with appmod.app.app_context():
                appmod.get_db().execute('UPDATE event_attendees SET paid=1 WHERE key=?',(key,));appmod.get_db().commit()
            payload = {'batch_id':'claims-1','claims':[claim]}
            accepted = self.meal_api(event,'claims',payload)
            self.assertEqual(accepted.status_code,200,accepted.json)
            self.assertEqual(accepted.json['results'][0]['status'],'accepted')
            self.assertEqual(self.meal_api(event,'claims',payload).json,accepted.json)
            self.assertEqual(self.meal_api(event,'claims',dict(payload,claims=[dict(claim,scanner_id='other')])).status_code,409)
            duplicate = self.meal_api(event,'claims',{'batch_id':'claims-2','claims':[self.meal_claim(key,'claim-2')]})
            self.assertEqual(duplicate.json['results'][0]['status'],'duplicate')
            audit = {'batch_id':'audit-1','audit':[dict(audit_id='correction-1',claim_id='claim-1',action='void_claim',reason='Operator correction',occurred_at='2026-12-20T18:10:00+08:00')]}
            self.assertEqual(self.meal_api(event,'audit',audit).status_code,200)
            self.assertEqual(self.meal_api(event,'audit',audit).status_code,200)
            fresh = self.meal_api(event,'claims',{'batch_id':'claims-3','claims':[self.meal_claim(key,'claim-3')]})
            self.assertEqual(fresh.json['results'][0]['status'],'accepted')
            with appmod.app.app_context():
                db = appmod.get_db()
                self.assertEqual(db.execute("SELECT COUNT(*) FROM event_meal_claims WHERE status='accepted'").fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM event_meal_audit').fetchone()[0],1)
                original = db.execute("SELECT * FROM event_meal_claims WHERE key='claim-1'").fetchone()
                self.assertEqual(original['status'],'voided')
                self.assertEqual(original['served_at'],'2026-12-20T10:00:00+00:00')
            details = self.client.get('/event-registration/admin/delegate/'+key).json
            self.assertEqual(len(details['meals']),4)
            self.assertEqual(details['audit'][0]['reason'],'Operator correction')
            self.assertEqual(self.meal_api(event,'sync-status').json['claim_counts']['accepted'],1)
            moved_date = {'batch_id':'move-used-session','sets':[dict(set_id='convention-meals',name='Changed',revision=2,
                sessions=[dict(session_id='opening-dinner',name='Dinner',date='2026-12-21',enabled=True)])]}
            self.assertEqual(self.meal_api(event,'meal-sets',moved_date).status_code,409)
            self.assertEqual(self.meal_api(event,'sync-status').json['sessions'][0]['date'],'2026-12-20')

    def test_meal_batch_validation_rollback_event_isolation_and_set_revisions(self):
        event = self.create_event()
        ticket,_ = self.register_details(event);self.capture(ticket)
        other = self.create_event('Youth Convention')
        foreign,_ = self.register_details(other);self.capture(foreign)
        with appmod.app.app_context():
            appmod.get_db().execute('UPDATE event_attendees SET paid=1');appmod.get_db().commit()
        with patch.dict(os.environ,EVENT_MEAL_MASTER_TOKEN='m'*64):
            self.assertEqual(self.meal_set(event).status_code,200)
            invalid = {'batch_id':'rollback','claims':[self.meal_claim(ticket),dict(self.meal_claim(ticket,'bad'),ticket_id='bad')]}
            self.assertEqual(self.meal_api(event,'claims',invalid).status_code,400)
            with appmod.app.app_context():
                self.assertEqual(appmod.get_db().execute('SELECT COUNT(*) FROM event_meal_claims').fetchone()[0],0)
            isolated = self.meal_api(event,'claims',{'batch_id':'foreign','claims':[self.meal_claim(foreign)]})
            self.assertEqual(isolated.json['results'][0]['status'],'invalid_ticket')
            self.assertEqual(self.meal_set(event,revision=2,enabled=False).status_code,200)
            self.assertEqual(self.meal_set(event).status_code,200)  # Exact retry returns original acknowledgement.
            stale = {'batch_id':'stale-set','sets':[{'set_id':'convention-meals','name':'Old','revision':1,'sessions':[]}]}
            self.assertEqual(self.meal_api(event,'meal-sets',stale).status_code,409)
            disabled = self.meal_api(event,'claims',{'batch_id':'disabled','claims':[self.meal_claim(ticket,'disabled')]})
            self.assertEqual(disabled.json['results'][0]['status'],'invalid_session')
            self.assertEqual(self.meal_api(event,'claims',{'batch_id':'naive','claims':[dict(self.meal_claim(ticket),served_at='2026-12-20T18:00:00')]}).status_code,400)

    def test_meal_duplicate_constraint_under_concurrent_master_requests(self):
        from concurrent.futures import ThreadPoolExecutor
        event = self.create_event()
        key,_ = self.register_details(event);self.capture(key)
        with appmod.app.app_context():
            appmod.get_db().execute('UPDATE event_attendees SET paid=1 WHERE key=?',(key,));appmod.get_db().commit()
        with patch.dict(os.environ,EVENT_MEAL_MASTER_TOKEN='m'*64):
            self.meal_set(event)
            def sync(index):
                client = appmod.app.test_client()
                return client.post('/event-registration/api/v1/events/'+event['key']+'/claims',
                    headers={'Authorization':'Bearer '+'m'*64},json={'batch_id':'race-'+str(index),'claims':[self.meal_claim(key,'race-'+str(index))]})
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(sync,range(2)))
            for index,result in enumerate(results):
                if result.status_code==503:
                    results[index] = sync(index)
            self.assertEqual([r.status_code for r in results],[200,200])
            self.assertEqual(sorted(r.json['results'][0]['status'] for r in results),['accepted','duplicate'])

    def test_meal_google_backup_batches_retries_and_preserves_local_records(self):
        event = self.create_event()
        key,_ = self.register_details(event);self.capture(key)
        with appmod.app.app_context():
            appmod.get_db().execute('UPDATE event_attendees SET paid=1');appmod.get_db().commit()
        with patch.dict(os.environ,EVENT_MEAL_MASTER_TOKEN='m'*64):
            self.meal_set(event)
            self.meal_api(event,'claims',{'batch_id':'export','claims':[self.meal_claim(key)]})
        values = []
        sheet = Mock(col_count=15)
        sheet.get_all_values.side_effect = lambda: copy.deepcopy(values)
        sheet.update.side_effect = lambda **kw: values.extend(copy.deepcopy(kw['values']))
        def append(rows,**kwargs):
            values.extend(copy.deepcopy(rows))
            if len(values)==2: raise TimeoutError('Lost acknowledgement')
        sheet.append_rows.side_effect = append
        sheet.batch_update.side_effect = lambda updates,**kwargs: [values.__setitem__(int(item['range'].split(':')[0][1:])-1,item['values'][0]) for item in updates]
        book = Mock();book.worksheet.return_value=sheet
        client = Mock();client.open.return_value=book
        url='/event-registration/admin/meals/backup'
        with patch.object(appmod,'get_gs_client',return_value=client):
            self.assertEqual(self.client.post(url,data={'csrf_token':self.csrf(),'event_id':event['id']}).status_code,503)
            response=self.client.post(url,data={'csrf_token':self.csrf(),'event_id':event['id']})
            self.assertEqual(response.status_code,200,response.json)
            self.assertEqual(len(values),2)
            self.assertEqual(values[1][-1],'accepted')
            self.assertEqual(sheet.append_rows.call_args.kwargs['value_input_option'],'RAW')
        with appmod.app.app_context():
            self.assertEqual(appmod.get_db().execute('SELECT COUNT(*) FROM event_meal_claims').fetchone()[0],1)


if __name__ == '__main__':
    unittest.main()
