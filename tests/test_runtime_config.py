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


if __name__ == "__main__":
    unittest.main()
