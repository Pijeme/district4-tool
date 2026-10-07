"""Safety checks use temporary SQLite databases and mocked Google Sheets only."""

import copy
import os
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

    def test_pastor_thumbnail_ten_day_cache(self):
        self.login(pastor=True)
        thumbnail=Path(self.directory.name)/'cover.png'
        thumbnail.write_bytes(b'fake-thumbnail')
        with patch.object(pastor_resources,'get_effective_book',return_value={'title':'Test'}), \
             patch.object(pastor_resources,'get_cached_thumbnail',return_value=(str(thumbnail),'image/png')):
            response=self.client.get('/pastor-resources/thumbnail/1')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.cache_control.max_age,864000)
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


if __name__ == '__main__':
    unittest.main()
