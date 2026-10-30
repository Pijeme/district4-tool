"""Check deployment credentials without using real keys or network services."""

import base64
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import runtime_config as config
import temp_edit


class RuntimeConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.info = {
            "type": "service_account",
            "client_email": "test@example.invalid",
            "token_uri": "https://oauth2.googleapis.com/token",
            "private_key": key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ).decode(),
        }
        cls.raw = json.dumps(cls.info)

    def setUp(self):
        env = patch.dict(os.environ, {}, clear=True)
        env.start()
        self.addCleanup(env.stop)

    def check_credentials(self, filename="missing.json"):
        credentials = config.google_credentials(filename, scopes=["test-scope"])
        self.assertEqual(credentials.service_account_email, "test@example.invalid")
        self.assertEqual(credentials.scopes, ["test-scope"])

    def test_base64_credentials_need_no_file(self):
        os.environ["GOOGLE_SERVICE_ACCOUNT_BASE64"] = base64.b64encode(
            self.raw.encode()
        ).decode()
        self.check_credentials()

    def test_json_credentials_preserve_private_key_newlines(self):
        os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"] = self.raw
        self.check_credentials()

    def test_existing_credentials_file_still_works(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = Path(directory) / "service_account.json"
            filename.write_text(self.raw, encoding="utf-8")
            self.check_credentials(str(filename))
            os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"] = str(filename)
            self.check_credentials()

    def test_malformed_env_does_not_expose_its_value(self):
        for variable, value in [
            ("GOOGLE_SERVICE_ACCOUNT_BASE64", "private-value-is-not-base64!"),
            ("GOOGLE_SERVICE_ACCOUNT_JSON", "private-value-is-not-json"),
            ("GOOGLE_SERVICE_ACCOUNT_JSON", "[]"),
        ]:
            with self.subTest(variable=variable, value=value):
                with patch.dict(os.environ, {variable: value}, clear=True):
                    with self.assertRaises(RuntimeError) as caught:
                        config.google_credentials("missing.json", scopes=[])
                    self.assertNotIn(value, str(caught.exception))

    def test_data_directory_is_created_outside_application(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "persistent"
            os.environ["DATA_DIR"] = str(data_dir)
            self.assertEqual(config.data_path("app_v2.db"), str(data_dir / "app_v2.db"))
            self.assertTrue(data_dir.is_dir())

    def test_local_storage_default_is_preserved(self):
        self.assertEqual(config.data_path("app_v2.db"), str(config.BASE_DIR / "app_v2.db"))


class HealthEndpointTests(unittest.TestCase):
    def test_health_does_not_sync_or_write_even_during_maintenance(self):
        import app as appmod
        import pastor_resources

        with patch.object(appmod, "init_db", side_effect=AssertionError("Schema write")), \
             patch.object(appmod, "sync_from_sheets_if_needed", side_effect=AssertionError("Sheets sync")), \
             patch.object(appmod, "_log_visit_if_needed", side_effect=AssertionError("Visit write")), \
             patch.object(pastor_resources, "_database_maintenance_active", return_value=True):
            response = appmod.app.test_client().get("/healthz")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json(), {"status": "ok"})


class OptionalTokenTests(unittest.TestCase):
    def test_blank_optional_tokens_cannot_authorize_requests(self):
        with patch.object(temp_edit, "TEMP_EDIT_USER_TOKEN", ""), \
             patch.object(temp_edit, "TEMP_EDIT_ADMIN_TOKEN", ""):
            for kind in ("user", "admin"):
                self.assertFalse(temp_edit._authorized("", kind))
                self.assertFalse(temp_edit._authorized("some-token", kind))

    def test_configured_tokens_still_authorize(self):
        with patch.object(temp_edit, "TEMP_EDIT_USER_TOKEN", "user-secret"), \
             patch.object(temp_edit, "TEMP_EDIT_ADMIN_TOKEN", "admin-secret"):
            self.assertTrue(temp_edit._authorized("user-secret", "user"))
            self.assertTrue(temp_edit._authorized("admin-secret", "admin"))
            self.assertFalse(temp_edit._authorized("user-secret", "admin"))


class CCMWorkflowTests(unittest.TestCase):
    """Local registration and verified Google backup, without live writes."""

    def setUp(self):
        import io
        import copy
        from unittest.mock import Mock
        from PIL import Image
        import app as appmod
        import pastor_resources
        self.mod = appmod
        self.original_upload = appmod.ccm_upload
        self.original_verify = appmod.ccm_verify_upload
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.rows = [list(appmod.CCM_HEADERS)]
        self.sheet = Mock()
        self.sheet.get_all_values.side_effect = lambda: copy.deepcopy(self.rows)
        self.sheet.append_row.side_effect = lambda row, **kwargs: self.rows.append(list(row))
        self.sheet.update.side_effect = lambda *, range_name, values, **kwargs: self.rows.__setitem__(int(range_name.split(':')[0][1:]) - 1, list(values[0]))
        self.cloud_files = {}
        def store_image(key, side, data):
            import hashlib
            file_id = 'drive_' + side + '_' + key + '_' + hashlib.sha256(data).hexdigest()[:12]
            self.cloud_files[file_id] = data
            return file_id
        self.store_image = store_image
        self.upload = Mock(side_effect=store_image)
        self.verify = Mock()
        for replacement in (
            patch.object(appmod, 'DATABASE', str(self.directory / 'app.db')),
            patch.object(appmod, 'data_path', side_effect=lambda name: str(self.directory / name)),
            patch.object(appmod, 'sync_from_sheets_if_needed', return_value=None),
            patch.object(appmod, '_log_visit_if_needed', return_value=None),
            patch.object(pastor_resources, '_database_maintenance_active', return_value=False),
            patch.object(appmod, 'ccm_sheet', return_value=self.sheet),
            patch.object(appmod, 'ccm_upload', self.upload),
            patch.object(appmod, 'ccm_verify_upload', self.verify),
            patch.object(appmod, 'ccm_cloud_asset', side_effect=lambda file_id: self.cloud_files[file_id]),
            patch.dict(appmod.app.config, TESTING=True, SECRET_KEY='ccm-isolated-test-session'),
        ):
            replacement.start()
            self.addCleanup(replacement.stop)
        with appmod.app.app_context():
            appmod.init_db()
            db = appmod.get_db()
            db.execute("INSERT INTO sheet_accounts_cache (username,name,age,sex,church_address,position,password) VALUES ('pastor-test','District Pastor','4','church-test','District Church Address','Pastor','district-private-password')")
            db.commit()
        self.client = appmod.app.test_client()
        image = Image.new('RGB', (600, 800), '#8eafcb')
        output = io.BytesIO()
        image.save(output, 'PNG')
        self.photo = output.getvalue()

    def login(self):
        self.client.get('/ccm?tab=admin')
        with self.client.session_transaction() as session:
            csrf = session['ccm_csrf']
        response = self.client.post('/ccm/admin/login', data={'csrf_token': csrf, 'password': 'CCMDistric4'})
        self.assertEqual(response.status_code, 302)
        self.client.get('/ccm?tab=admin')

    def payload(self, *, editing=None, name='Test CCM Worker', photo=True):
        import io
        with self.client.session_transaction() as session:
            csrf = session['ccm_csrf']
            form_key = session.get('ccm_draft', {}).get('key', '')
        with self.mod.app.app_context():
            church = self.mod.ccm_churches()[0]
        data = {'csrf_token': csrf, 'form_key': form_key, 'name': name, 'area': '4', 'church_key': church['key'],
                'pastor': 'Forged Pastor', 'campus': 'Test Campus', 'phone': '0123456789', 'email': 'test@example.invalid', 'status': 'Active'}
        if editing:
            data.update(record_key=editing['Record Key'], revision=editing['Updated At'])
        if photo:
            data['photo'] = (io.BytesIO(self.photo), 'photo.png')
        return data

    def create(self):
        response = self.client.post('/ccm/admin/save', data=self.payload())
        self.assertEqual(response.status_code, 200, response.data)
        with self.client.session_transaction() as session:
            key = session['ccm_last_created']
        with self.mod.app.app_context():
            return self.mod.ccm_pending()[key]

    def backup(self, worker):
        with self.client.session_transaction() as session:
            csrf = session['ccm_csrf']
        return self.client.post('/ccm/admin/backup/' + worker['Record Key'], headers={'X-CSRF-Token': csrf})

    def pending(self):
        with self.mod.app.app_context():
            return self.mod.ccm_pending()

    def local_images(self, worker):
        return list((self.directory / 'ccm_assets').glob(worker['Record Key'] + '-*.png'))

    def test_public_pages_and_all_admin_actions_enforce_access(self):
        self.assertEqual(self.client.get('/ccm').status_code, 200)
        self.assertEqual(self.client.get('/ccm?tab=workers').status_code, 200)
        self.assertNotIn(b'district-private-password', self.client.get('/ccm?tab=workers').data)
        for path in ['/ccm/admin/save', '/ccm/admin/preview', '/ccm/admin/delete/' + 'a' * 32,
                     '/ccm/admin/backup', '/ccm/admin/backup/' + 'a' * 32, '/ccm/admin/refresh']:
            self.assertEqual(self.client.post(path).status_code, 403)
        self.login()
        self.assertEqual(self.client.post('/ccm/admin/save', data={'name': 'Missing CSRF'}).status_code, 400)
        self.assertNotIn(b'CCMDistric4', self.client.get('/ccm?tab=admin').data)

    def test_preview_is_not_a_registration_or_drive_upload(self):
        self.login()
        response = self.client.post('/ccm/admin/preview', data=self.payload())
        self.assertEqual(response.status_code, 200)
        images = response.get_json()['images']
        self.assertEqual(set(images), {'front', 'back'})
        self.assertTrue(all(value.startswith('data:image/png;base64,') for value in images.values()))
        self.upload.assert_not_called()
        self.assertEqual(len(self.rows), 1)

    def test_registration_is_local_and_saves_separate_pngs(self):
        import io
        from PIL import Image
        self.login()
        worker = self.create()
        self.assertEqual(worker['Pastor'], 'District Pastor')
        self.assertEqual(worker['Church Address'], 'District Church Address')
        self.assertTrue(worker['ID Number'].startswith('CCM-'))
        self.assertTrue(worker['Created At'])
        self.upload.assert_not_called()
        self.assertEqual(len(self.rows), 1)
        self.assertEqual(len(self.local_images(worker)), 3)
        for side in ('front', 'back'):
            response = self.client.get('/ccm/image/' + worker['Record Key'] + '/' + side + '.png?download=1')
            self.assertEqual(response.status_code, 200)
            self.assertIn('-' + side + '.png', response.headers['Content-Disposition'])
            image = Image.open(io.BytesIO(response.data))
            self.assertEqual((image.format, image.size), ('PNG', (1050, 1500)))
        self.client.post('/ccm/admin/logout', data={'csrf_token': self.payload(photo=False)['csrf_token']})
        self.assertEqual(self.client.get('/ccm/image/' + worker['Record Key'] + '/front.png').status_code, 403)
        self.assertEqual(self.client.get('/ccm/image/' + worker['Record Key'] + '/photo.png').status_code, 200)
        directory = self.client.get('/ccm?tab=workers').data
        for value in ('Test CCM Worker', 'Test Campus', 'District Pastor', 'test@example.invalid', worker['ID Number']):
            self.assertIn(value.encode(), directory)

    def test_edit_retains_id_and_created_date_and_rejects_stale_edits(self):
        self.login()
        old = self.create()
        data = self.payload(editing=old, name='Updated Worker', photo=False)
        response = self.client.post('/ccm/admin/save', data=data)
        self.assertEqual(response.status_code, 200, response.data)
        current = self.pending()[old['Record Key']]
        self.assertEqual(current['Name'], 'Updated Worker')
        self.assertEqual(current['ID Number'], old['ID Number'])
        self.assertEqual(current['Created At'], old['Created At'])
        self.assertNotEqual(current['Updated At'], old['Updated At'])
        response = self.client.post('/ccm/admin/save', data=data)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(len(self.rows), 1)

    def test_delete_removes_public_access_and_invalidates_qr(self):
        self.login()
        worker = self.create()
        csrf = self.payload(photo=False)['csrf_token']
        response = self.client.post('/ccm/admin/delete/' + worker['Record Key'], data={'csrf_token': csrf})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.pending()[worker['Record Key']]['Status'], 'Deleted')
        self.assertNotIn(b'Test CCM Worker', self.client.get('/ccm?tab=workers').data)
        self.assertEqual(self.client.get('/ccm/verify/' + worker['Record Key']).status_code, 404)
        self.assertEqual(self.client.get('/ccm/image/' + worker['Record Key'] + '/photo.png').status_code, 404)

    def test_google_outage_does_not_block_local_registration(self):
        self.login()
        self.upload.side_effect = self.mod.CCMStorageError('Drive unavailable')
        self.mod.ccm_sheet.side_effect = self.mod.CCMStorageError('Sheets unavailable')
        worker = self.create()
        self.assertEqual(self.backup(worker).status_code, 503)
        self.assertEqual(len(self.rows), 1)
        self.assertIn(worker['Record Key'], self.pending())
        self.assertEqual(len(self.local_images(worker)), 3)
        self.assertIn(b'Test CCM Worker', self.client.get('/ccm?tab=workers').data)

    def test_retry_after_lost_sheets_acknowledgement_recovers_one_account(self):
        self.login()
        worker = self.create()
        form_key = worker['Record Key']
        def append_then_timeout(row, **kwargs):
            self.rows.append(list(row))
            raise TimeoutError('Acknowledgement lost')
        self.sheet.append_row.side_effect = append_then_timeout
        self.assertEqual(self.backup(worker).status_code, 503)
        self.assertEqual(len(self.local_images(worker)), 3)
        response = self.backup(worker)
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(len(self.rows), 2)
        self.assertEqual(len(self.cloud_files), 3)
        self.assertEqual(self.local_images(worker), [])
        self.assertNotIn(form_key, self.pending())
        self.assertEqual(self.client.get('/ccm/print/' + form_key).status_code, 200)
        # A repeated POST after the success cookie is set is also idempotent.
        again = self.payload(photo=False)
        again['form_key'] = form_key
        self.assertEqual(self.client.post('/ccm/admin/save', data=again).status_code, 200)
        self.assertEqual(len(self.rows), 2)

    def test_stale_directory_cannot_confirm_an_active_qr(self):
        self.login()
        worker = self.create()
        self.assertEqual(self.backup(worker).status_code, 200)
        response = self.client.get('/ccm/verify/' + worker['Record Key'])
        self.assertIn(b'Active worker ID', response.data)
        self.mod.ccm_sheet.side_effect = self.mod.CCMStorageError('Offline')
        response = self.client.get('/ccm/verify/' + worker['Record Key'])
        self.assertIn(b'Verification unavailable', response.data)
        self.assertNotIn(b'Active worker ID', response.data)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')

    def test_owner_upload_connection_is_authenticated_and_rejects_other_urls(self):
        from unittest.mock import Mock
        reply = Mock()
        reply.json.return_value = {'ok': True, 'file_id': 'owner_uploaded_file'}
        settings = {'CCM_DRIVE_SCRIPT_URL': 'https://script.google.com/macros/s/test_deployment/exec',
                    'CCM_DRIVE_API_TOKEN': 'private-upload-token-' * 3}
        with patch.dict(os.environ, settings), patch.object(self.mod.requests, 'post', return_value=reply) as post:
            self.assertEqual(self.original_upload('a' * 32, 'front', self.photo), 'owner_uploaded_file')
            request = post.call_args.kwargs['json']
            self.assertEqual(request['token'], settings['CCM_DRIVE_API_TOKEN'])
            self.assertNotEqual(request['token'], 'CCMDistric4')
            self.assertEqual(request['side'], 'front')
        settings['CCM_DRIVE_SCRIPT_URL'] = 'https://example.invalid/upload'
        with patch.dict(os.environ, settings), patch.object(self.mod.requests, 'post') as post:
            with self.assertRaises(self.mod.CCMStorageError):
                self.original_upload('a' * 32, 'front', self.photo)
            post.assert_not_called()

    def test_manual_text_and_original_artwork_are_preserved(self):
        import io
        from PIL import Image, ImageChops
        from pypdf import PdfReader
        root = Path(self.mod.app.static_folder) / 'ccm'
        manual = json.loads((root / 'manual.json').read_text(encoding='utf-8'))
        self.assertEqual(manual['pages'], [p.extract_text() or '' for p in PdfReader(root / 'CCM Manual of Operation.pdf').pages])
        for field in ('introduction', 'vision', 'mission'):
            self.assertIn(manual[field], ''.join(manual['pages']))
        self.login()
        worker = self.create()
        rendered = self.mod.ccm_render_id(worker, self.photo)
        allowed = {'front': [(332,566,718,954),(120,975,936,1069),(170,1095,920,1143),(378,1174,926,1307)],
                   'back': [(80,1140,315,1380),(375,1202,930,1322)]}
        for side in ('front', 'back'):
            original = Image.open(root / ('id-' + side + '-template.jpg')).convert('RGB')
            result = Image.open(io.BytesIO(rendered[side])).convert('RGB')
            difference = ImageChops.difference(original, result)
            for box in allowed[side]:
                difference.paste((0, 0, 0), box)
            self.assertIsNone(difference.getbbox(), 'Artwork outside variable fields changed on ' + side)

    def test_invalid_password_and_rate_limit_cannot_unlock_admin(self):
        self.client.get('/ccm?tab=admin')
        with self.client.session_transaction() as session:
            csrf = session['ccm_csrf']
        for password in ['incorrect', '\N{SNOWMAN}'] + ['incorrect'] * 8 + ['CCMDistric4']:
            response = self.client.post('/ccm/admin/login', data={'csrf_token': csrf, 'password': password})
            self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as session:
            self.assertNotIn('ccm_admin_until', session)

    def test_sheet_read_outage_uses_cache_without_attesting_validity(self):
        self.login()
        worker = self.create()
        self.assertEqual(self.backup(worker).status_code, 200)
        self.sheet.get_all_values.side_effect = TimeoutError('Read unavailable')
        response = self.client.get('/ccm/verify/' + worker['Record Key'])
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Verification unavailable', response.data)
        self.assertNotIn(b'Active worker ID', response.data)

    def test_large_phone_jpeg_is_resized_instead_of_rejected(self):
        import io
        from PIL import Image
        from werkzeug.datastructures import FileStorage
        raw = io.BytesIO()
        source = Image.new('RGB', (6000, 4000), '#85a3bd')
        source.save(raw, 'JPEG')
        source.close()
        raw.seek(0)
        prepared = self.mod.ccm_photo(FileStorage(stream=raw, filename='phone.jpg'))
        with Image.open(io.BytesIO(prepared)) as image:
            self.assertLessEqual(max(image.size), 1400)
            self.assertEqual(image.format, 'PNG')

    def test_successful_backup_verifies_images_clears_local_and_reads_google_later(self):
        self.login()
        worker = self.create()
        self.assertEqual(self.backup(worker).status_code, 200)
        self.assertEqual(self.verify.call_count, 3)
        self.assertEqual(self.local_images(worker), [])
        self.assertNotIn(worker['Record Key'], self.pending())
        self.assertEqual(len(self.rows), 2)
        directory = self.client.get('/ccm?tab=workers')
        self.assertIn(b'Test CCM Worker', directory.data)
        image = self.client.get('/ccm/image/' + worker['Record Key'] + '/front.png')
        self.assertEqual(image.status_code, 200)
        self.assertEqual(self.local_images(worker), [], 'Cloud reads must not recreate local image files')
        page = self.client.get('/ccm?tab=admin&mode=manage')
        self.assertIn(b'Backed up to Google', page.data)

    def test_editing_a_backed_up_account_keeps_id_and_updates_the_same_sheet_row(self):
        self.login()
        worker = self.create()
        self.assertEqual(self.backup(worker).status_code, 200)
        cloud = dict(zip(self.mod.CCM_HEADERS, self.rows[-1]))
        response = self.client.post('/ccm/admin/save', data=self.payload(editing=cloud, name='Edited After Backup', photo=False))
        self.assertEqual(response.status_code, 200, response.data)
        edited = self.pending()[worker['Record Key']]
        self.assertEqual(edited['ID Number'], worker['ID Number'])
        self.assertEqual(edited['Created At'], worker['Created At'])
        self.assertEqual(self.backup(edited).status_code, 200)
        self.assertEqual(len(self.rows), 2)
        self.assertEqual(self.rows[-1][2], 'Edited After Backup')

    def test_checksum_failure_keeps_every_local_image_and_record(self):
        self.login()
        worker = self.create()
        self.verify.side_effect = self.mod.CCMStorageError('Checksum not confirmed')
        self.assertEqual(self.backup(worker).status_code, 503)
        self.assertEqual(len(self.local_images(worker)), 3)
        self.assertIn(worker['Record Key'], self.pending())
        self.assertEqual(len(self.rows), 1)

    def test_google_image_verification_checks_size_and_checksum(self):
        from unittest.mock import Mock
        reply = Mock()
        reply.json.return_value = {'size': len(self.photo), 'md5Checksum': 'incorrect', 'trashed': False}
        with patch.object(self.mod, 'ccm_drive', return_value=reply):
            with self.assertRaises(self.mod.CCMStorageError):
                self.original_verify('file_id', self.photo)

    def test_cleanup_failure_can_be_retried_without_reuploading_images(self):
        self.login()
        worker = self.create()
        original_unlink = Path.unlink
        def fail_on_front(path, *args, **kwargs):
            if worker['Record Key'] in path.name and '-front-' in path.name:
                raise PermissionError('Image is temporarily locked')
            return original_unlink(path, *args, **kwargs)
        with patch.object(Path, 'unlink', fail_on_front):
            self.assertEqual(self.backup(worker).status_code, 503)
        pending = self.pending()[worker['Record Key']]
        self.assertEqual(pending['_backup_confirmed_revision'], pending['Updated At'])
        self.assertEqual(self.upload.call_count, 3)
        self.assertEqual(self.backup(pending).status_code, 200)
        self.assertEqual(self.upload.call_count, 3)
        self.assertEqual(self.local_images(worker), [])

    def test_edit_during_backup_is_preserved_for_the_next_backup(self):
        self.login()
        worker = self.create()
        changed = []
        def upload_while_editing(key, side, body):
            if side == 'photo' and not changed:
                changed.append(True)
                with self.mod.app.app_context():
                    latest = dict(worker)
                    latest['Name'] = 'Newer Local Edit'
                    latest['Updated At'] = '2099-01-01T00:00:00+00:00'
                    images = {'photo': self.photo, **self.mod.ccm_render_id(latest, self.photo)}
                    self.mod.ccm_store_local(latest, images, expected_revision=worker['Updated At'])
            return self.store_image(key, side, body)
        self.upload.side_effect = upload_while_editing
        response = self.backup(worker)
        self.assertEqual(response.status_code, 200, response.data)
        self.assertFalse(response.get_json()['cleaned'])
        latest = self.pending()[worker['Record Key']]
        self.assertEqual(latest['Name'], 'Newer Local Edit')
        self.assertEqual(latest['_cloud_revision'], worker['Updated At'])
        self.assertTrue(self.local_images(worker))
        self.upload.side_effect = self.store_image
        self.assertEqual(self.backup(latest).status_code, 200)
        self.assertEqual(self.rows[-1][2], 'Newer Local Edit')
        self.assertEqual(self.local_images(worker), [])

    def test_external_google_edit_is_not_overwritten_by_a_local_backup(self):
        self.login()
        worker = self.create()
        self.assertEqual(self.backup(worker).status_code, 200)
        cloud = dict(zip(self.mod.CCM_HEADERS, self.rows[-1]))
        response = self.client.post('/ccm/admin/save', data=self.payload(editing=cloud, name='Local Edit', photo=False))
        self.assertEqual(response.status_code, 200)
        self.rows[-1][2] = 'External Google Edit'
        self.rows[-1][12] = '2098-01-01T00:00:00+00:00'
        self.assertEqual(self.backup(worker).status_code, 503)
        self.assertEqual(self.rows[-1][2], 'External Google Edit')
        self.assertIn(worker['Record Key'], self.pending())
        self.assertTrue(self.local_images(worker))

    def test_local_qr_stays_available_without_google_before_backup(self):
        self.login()
        worker = self.create()
        self.mod.ccm_sheet.side_effect = self.mod.CCMStorageError('Offline')
        response = self.client.get('/ccm/verify/' + worker['Record Key'])
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Active worker ID', response.data)

    def test_backup_updates_correct_google_row_when_blank_rows_exist(self):
        self.login()
        worker = self.create()
        self.assertEqual(self.backup(worker).status_code, 200)
        cloud = dict(zip(self.mod.CCM_HEADERS, self.rows[-1]))
        self.rows.insert(1, [])
        response = self.client.post('/ccm/admin/save', data=self.payload(editing=cloud, name='After Blank Row', photo=False))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.backup(worker).status_code, 200)
        self.assertEqual(self.rows[1], [])
        self.assertEqual(self.rows[2][2], 'After Blank Row')


if __name__ == "__main__":
    unittest.main()
