"""Scanner integration checks against the real local website, isolated SQLite only.

Run: .venv/Scripts/python -m unittest discover -s tests -p test_scanner_sync.py -v
"""
import importlib
import os
import sqlite3
import tempfile
import unittest
import sys
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

with tempfile.TemporaryDirectory() as initial:
    with patch.dict(os.environ, {'DATA_DIR': initial}):
        appmod = importlib.import_module('app')

PREFIX = '/api/v2/meal-scanner'
EVENT = 'a' * 32
PAID = 'b' * 32
OTHER = 'c' * 32
DEVICE = '11111111-1111-1111-1111-111111111111'
DAY = '2026-11-10'
NOW = '2026-11-10T02:00:00+00:00'


class ScannerSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = str(Path(self.temp.name) / 'scanner.db')
        for replacement in (
            patch.object(appmod, 'DATABASE', self.database),
            patch.object(appmod, 'sync_from_sheets_if_needed', side_effect=AssertionError('Unexpected Sheets call')),
            patch.object(appmod, '_log_visit_if_needed', side_effect=AssertionError('Unexpected visit log')),
            patch.dict(appmod.app.config, TESTING=True, SECRET_KEY='scanner-test'),
        ):
            replacement.start()
            self.addCleanup(replacement.stop)
        self.client = appmod.app.test_client()
        with appmod.app.app_context():
            appmod.event_tables()
            appmod.event_meal_tables()
            db = appmod.get_db()
            db.execute('''INSERT INTO registration_events
                (id,key,name,kind,start_date,end_date,theme,scripture,background,created_at,updated_at)
                VALUES(1,?,'Test event','District Convention',?,?,'Serve together','John 6:35','',?,?)''',
                (EVENT, DAY, '2026-11-12', NOW, NOW))
            for key, number, paid in [(PAID, 1, 1), (OTHER, 2, 0)]:
                db.execute('''INSERT INTO event_attendees
                    (key,event_id,number,name,birthday,area,church,address,pastor,created_at,updated_at,complete,paid)
                    VALUES(?,1,?,'Test attendee','2000-01-01','7','Test church','','',?,?,1,?)''',
                    (key, number, NOW, NOW, paid))
            db.commit()
        # Retrieve the expected test password from the owned website function's
        # constant, without logging it or copying it into test artifacts.
        self.enroll()

    def enroll(self):
        verifier = appmod.event_password_matches
        password = next(v.decode() for v in verifier.__code__.co_consts if isinstance(v, bytes))
        response = self.client.post(PREFIX + '/enroll', json={
            'device_id': DEVICE, 'label': 'Isolated test', 'admin_password': password})
        self.assertEqual(response.status_code, 200, response.get_json())
        self.headers = {'X-IOWO-Device-ID': DEVICE,
                        'Authorization': 'Bearer ' + response.get_json()['access_token']}

    def get(self, suffix):
        return self.client.get(PREFIX + suffix, headers=self.headers)

    def post(self, suffix, payload):
        return self.client.post(PREFIX + '/events/' + EVENT + suffix,
                                json=payload, headers=self.headers)

    def execute(self, sql, args=()):
        with closing(sqlite3.connect(self.database)) as db, db:
            db.execute(sql, args)

    def test_authentication_and_fixed_five_day_expiry(self):
        self.assertEqual(self.client.get(PREFIX + '/events').status_code, 401)
        self.assertEqual(self.get('/events').status_code, 200)
        expired = (datetime.now(timezone.utc) - timedelta(days=5, seconds=1)).isoformat()
        self.execute('UPDATE meal_scanner_download_devices SET authorized_at=?', (expired,))
        self.assertEqual(self.get('/events').status_code, 401)
        self.enroll()
        self.assertEqual(self.get('/events').status_code, 200)
        self.execute("UPDATE meal_scanner_download_devices SET revoked_at='revoked'")
        self.assertEqual(self.get('/events').status_code, 401)

    def test_real_delta_add_edit_unpay_and_delete_without_bulk_resend(self):
        suffix = '/events/' + EVENT + '/paid-registrations'
        full = self.get(suffix).get_json()
        self.assertEqual(full['mode'], 'full')
        self.assertEqual([p['key'] for p in full['attendees']], [PAID])
        unchanged = self.get(suffix + '?cursor=' + full['cursor']).get_json()
        self.assertFalse(unchanged['changed'])
        self.assertEqual(unchanged['attendees'], [])
        self.execute('UPDATE event_attendees SET paid=1, revision=revision+1 WHERE key=?', (OTHER,))
        added = self.get(suffix + '?cursor=' + full['cursor']).get_json()
        self.assertEqual(added['mode'], 'delta')
        self.assertEqual([p['key'] for p in added['attendees']], [OTHER])
        self.execute("UPDATE event_attendees SET name='Changed',revision=revision+1 WHERE key=?", (PAID,))
        changed = self.get(suffix + '?cursor=' + added['cursor']).get_json()
        self.assertEqual(len(changed['attendees']), 1)
        self.assertEqual(changed['attendees'][0]['name'], 'Changed')
        self.execute('UPDATE event_attendees SET paid=0 WHERE key=?', (PAID,))
        removed = self.get(suffix + '?cursor=' + changed['cursor']).get_json()
        self.assertEqual(removed['removed_keys'], [PAID])
        self.assertEqual(removed['attendees'], [])
        self.execute("UPDATE event_attendees SET deleted_at='deleted' WHERE key=?", (OTHER,))
        deleted = self.get(suffix + '?cursor=' + removed['cursor']).get_json()
        self.assertEqual(deleted['removed_keys'], [OTHER])
        self.assertEqual(deleted['total'], 0)

    def meal_set(self, enabled=True, revision=1):
        result = self.post('/meal-sets', {'batch_id': 'set-' + str(revision), 'sets': [{
            'set_id': 'android-set', 'name': 'Event meals', 'revision': revision,
            'sessions': [{'session_id': 'breakfast', 'name': 'Breakfast', 'date': DAY, 'enabled': enabled}]}]})
        self.assertEqual(result.status_code, 200, result.get_json())

    def claim(self, claim='claim-1', batch='claim-batch', ticket=PAID):
        return self.post('/claims', {'batch_id': batch, 'claims': [{
            'claim_id': claim, 'session_id': 'breakfast', 'ticket_id': ticket,
            'scanner_id': 'volunteer-one', 'served_at': NOW}]})

    def test_upload_retry_void_and_rescan_use_existing_website_tables(self):
        self.meal_set()
        first = self.claim()
        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(first.get_json()['results'][0]['status'], 'accepted')
        retry = self.claim()
        self.assertEqual(retry.get_json(), first.get_json())
        duplicate = self.claim('claim-2', 'different-batch')
        self.assertEqual(duplicate.get_json()['results'][0]['status'], 'duplicate')
        audit = {'batch_id': 'void-batch', 'audit': [{'audit_id': 'void-1', 'claim_id': 'claim-1',
            'action': 'void_claim', 'reason': 'Correct accidental scan', 'occurred_at': NOW}]}
        self.assertEqual(self.post('/audit', audit).status_code, 200)
        self.assertEqual(self.post('/audit', audit).status_code, 200)
        rescanned = self.claim('claim-3', 'replacement-batch')
        self.assertEqual(rescanned.get_json()['results'][0]['status'], 'accepted')
        status = self.get('/events/' + EVENT + '/sync-status').get_json()
        self.assertEqual(status['claim_counts'], {'accepted': 1, 'voided': 1, 'duplicate': 1})
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM event_meal_audit').fetchone()[0], 1)

    def test_disabled_meal_preserves_historical_upload_and_unpaid_is_reported(self):
        self.meal_set(enabled=False)
        self.assertEqual(self.claim().get_json()['results'][0]['status'], 'accepted')
        unpaid = self.claim('unpaid-claim', 'unpaid-batch', OTHER)
        self.assertEqual(unpaid.get_json()['results'][0]['status'], 'unpaid')

    def test_batch_conflicts_rollback_and_errors_are_json(self):
        self.meal_set()
        self.claim()
        conflict = self.claim('different-claim', 'claim-batch')
        self.assertEqual(conflict.status_code, 409)
        self.assertIsInstance(conflict.get_json()['error'], str)
        self.assertEqual(self.get('/missing-route').status_code, 404)
        self.assertTrue(self.get('/missing-route').is_json)
        self.execute("UPDATE registration_events SET deleted_at='deleted'")
        self.assertEqual(self.get('/events/' + EVENT + '/paid-registrations').status_code, 410)
        self.assertEqual(self.claim('deleted', 'deleted-batch').status_code, 404)

    def unlock_event_admin(self):
        for replacement in (
            patch.object(appmod, '_log_visit_if_needed', return_value=None),
            patch.dict(os.environ, {'DATA_DIR': self.temp.name}),
        ):
            replacement.start()
            self.addCleanup(replacement.stop)
        with self.client.session_transaction() as session:
            session['event_admin_until'] = 9999999999
            session['event_csrf'] = 'event-test-csrf'

    def event_post(self, suffix, data=None):
        return self.client.post('/event-registration/admin/' + suffix,
            data=data or {}, headers={'X-CSRF-Token': 'event-test-csrf'})

    def test_public_registration_progress_is_private_read_only_and_change_driven(self):
        url = '/event-registration/progress?list=1'
        with patch.object(appmod, 'event_tables', side_effect=AssertionError('Poll attempted schema writes')):
            first = self.client.get(url).get_json()
            self.assertEqual(first['count'], 2)
            self.assertEqual(set(first['attendees'][0]), {'number', 'name', 'area', 'church'})
            unchanged = self.client.get(url + '&cursor=' + first['cursor']).get_json()
            self.assertFalse(unchanged['changed'])
            self.assertNotIn('attendees', unchanged)
            self.assertNotIn('count', unchanged)
        self.execute("UPDATE event_attendees SET complete=0 WHERE key=?", (OTHER,))
        changed = self.client.get(url + '&cursor=' + first['cursor']).get_json()
        self.assertTrue(changed['changed'])
        self.assertEqual(changed['count'], 1)
        self.assertEqual(changed['attendees'][0]['number'], 1)
        private = self.client.get('/event-registration/progress?admin=1&event=1')
        self.assertEqual(private.status_code, 403)

    def test_physical_release_requires_payment_physical_print_and_confirmation(self):
        self.unlock_event_admin()
        # Old print/download marks must not allow a physical release.
        self.execute('UPDATE event_attendees SET printed=1 WHERE key=?', (OTHER,))
        status = 'status/' + OTHER
        release = {'field': 'released', 'value': 1, 'confirm': 'yes', 'revision': 1}
        self.assertEqual(self.event_post(status, release).status_code, 400)
        self.assertEqual(self.event_post(status, {'field': 'paid', 'value': 1, 'revision': 1}).status_code, 200)
        release['revision'] = 2
        self.assertEqual(self.event_post(status, release).status_code, 400)
        physical = {'field': 'physical_printed', 'value': 1, 'revision': 2}
        self.assertEqual(self.event_post(status, physical).status_code, 400)
        physical['physical_confirmed'] = 'yes'
        self.assertEqual(self.event_post(status, physical).status_code, 200)
        # Another admin's stale release request cannot overwrite current data.
        self.assertEqual(self.event_post(status, release).status_code, 409)
        release['revision'] = 3
        release.pop('confirm')
        self.assertEqual(self.event_post(status, release).status_code, 400)
        release['confirm'] = 'yes'
        self.assertEqual(self.event_post(status, release).status_code, 200)
        for field in ('paid', 'physical_printed'):
            blocked = self.event_post(status, {'field': field, 'value': 0, 'confirm': 'yes', 'revision': 4})
            self.assertEqual(blocked.status_code, 400)
        self.assertEqual(self.event_post(status, {'field': 'released', 'value': 0, 'confirm': 'yes', 'revision': 4}).status_code, 200)

    def test_shared_picture_is_temporary_until_physical_print_and_never_releases(self):
        self.unlock_event_admin()
        from PIL import Image
        import io
        image = io.BytesIO()
        Image.new('RGB', (30, 30), 'white').save(image, 'PNG')
        with appmod.app.app_context():
            filename = appmod.event_asset_write(image.getvalue())
        self.execute('UPDATE event_attendees SET photo=?,printed=1 WHERE key=?', (filename, PAID))
        with patch.object(appmod, 'event_render_id', return_value=image.getvalue()) as render:
            self.assertEqual(self.client.get('/event-registration/image/' + PAID + '/shared.png').status_code, 200)
            self.assertTrue(render.call_args.kwargs['temporary'])
            self.client.get('/event-registration/image/' + PAID + '/id.png')
            self.assertFalse(render.call_args.kwargs['temporary'])
            self.execute('UPDATE event_attendees SET physical_printed=1 WHERE key=?', (PAID,))
            self.client.get('/event-registration/image/' + PAID + '/shared.png')
            self.assertFalse(render.call_args.kwargs['temporary'])
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute('SELECT released FROM event_attendees WHERE key=?', (PAID,)).fetchone()[0], 0)

    def test_local_deletion_updates_counts_and_scanner_and_retries_cloud_cleanup(self):
        self.unlock_event_admin()
        public = self.client.get('/event-registration/progress?list=1').get_json()
        scanner = self.get('/events/' + EVENT + '/paid-registrations').get_json()
        response = self.event_post('delete/' + PAID, {'confirm': 'yes', 'revision': 1})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['pending_deletions'], 1)
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM event_attendees WHERE key=?', (PAID,)).fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM event_deletion_queue').fetchone()[0], 1)
        self.assertEqual(self.event_post('delete/' + PAID, {'confirm': 'yes', 'revision': 1}).status_code, 200)
        updated = self.client.get('/event-registration/progress?list=1&cursor=' + public['cursor']).get_json()
        self.assertEqual(updated['count'], 1)
        self.assertEqual(updated['attendees'][0]['number'], 2)
        delta = self.get('/events/' + EVENT + '/paid-registrations?cursor=' + scanner['cursor']).get_json()
        self.assertEqual(delta['removed_keys'], [PAID])
        self.assertEqual(self.client.get('/event-registration/ticket/' + PAID).status_code, 404)
        self.assertEqual(self.event_post('backup', {'event_id': 1}).get_json()['keys'][0], PAID)
        headers = appmod.EVENT_HEADERS
        remote = {'Name': 'Test attendee', 'Bday': '2000-01-01', 'Date created': NOW, 'Event': 'District Convention 2026'}
        values = [headers[:], [remote.get(h, '') for h in headers]]
        sheet = Mock()
        sheet.get_all_values.side_effect = lambda: [row[:] for row in values]
        sheet.delete_rows.side_effect = lambda number: values.pop(number - 1)
        failed = Mock()
        failed.json.return_value = {'ok': False, 'code': 'DRIVE'}
        confirmed = Mock()
        confirmed.json.return_value = dict(ok=True, deleted=True, purpose='event', event_key=EVENT,
                                          record_key=PAID, folder_id=appmod.EVENT_FOLDER_ID)
        env = {'EVENT_DRIVE_SCRIPT_URL': 'https://script.google.com/macros/s/test/exec', 'EVENT_DRIVE_API_TOKEN': 'x' * 32}
        with patch.dict(os.environ, env), patch.object(appmod.requests, 'post', side_effect=[failed, confirmed, confirmed]) as drive, \
                patch.object(appmod, 'event_sheet', return_value=(sheet, headers)):
            first = self.event_post('backup/' + PAID)
            self.assertEqual(first.status_code, 503)
            sheet.delete_rows.assert_not_called()
            with closing(sqlite3.connect(self.database)) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM event_deletion_queue').fetchone()[0], 1)
            def lost_acknowledgement(number):
                values.pop(number - 1)
                raise appmod.requests.ConnectionError('Simulated lost Sheets acknowledgement')
            sheet.delete_rows.side_effect = lost_acknowledgement
            second = self.event_post('backup/' + PAID)
            self.assertEqual(second.status_code, 503)
            with closing(sqlite3.connect(self.database)) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM event_deletion_queue').fetchone()[0], 1)
            third = self.event_post('backup/' + PAID)
            self.assertEqual(third.status_code, 200, third.get_json())
            self.assertEqual(third.get_json()['pending_deletions'], 0)
            self.assertEqual(drive.call_args.kwargs['json']['action'], 'delete')
            self.assertEqual(drive.call_args.kwargs['json']['record_key'], PAID)
            sheet.delete_rows.assert_called_once_with(2)
        self.assertTrue(self.event_post('backup/' + PAID).get_json()['removed'])

    def test_event_pages_render_and_protected_actions_require_admin(self):
        with patch.object(appmod, '_log_visit_if_needed', return_value=None), \
                patch.object(appmod, 'event_directory', return_value=([], [])), \
                patch.object(appmod, 'is_cache_developer', return_value=False):
            # Safari fetches the JSON web manifest as a direct file response.
            # Activity logging must not try to buffer that response to decode JSON.
            with self.client.get('/static/manifest.json') as manifest:
                self.assertEqual(manifest.status_code, 200)
            page = self.client.get('/event-registration?tab=attendees')
            self.assertEqual(page.status_code, 200)
            self.assertNotIn(PAID.encode(), page.data)
            self.assertNotIn(b'data-status-url', page.data)
            self.assertEqual(self.client.get('/event-registration/release/' + PAID).status_code, 302)
            self.unlock_event_admin()
            for path in ('?tab=admin', '/print/' + PAID, '/release/' + PAID):
                self.assertEqual(self.client.get('/event-registration' + path).status_code, 200)


if __name__ == '__main__':
    import sys
    if '--serve-sandbox' in sys.argv:
        # Optional disposable server for Android integration tests. Uses the
        # real website handlers; fixture DB and mock network hooks stay isolated.
        sandbox = ScannerSyncTests()
        sandbox.setUp()
        try:
            appmod.app.run(host='127.0.0.1', port=5056, debug=False, use_reloader=False)
        finally:
            sandbox.doCleanups()
    else:
        unittest.main()
