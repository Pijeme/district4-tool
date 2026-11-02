"""Shared storage paths and Google credentials for local and container runs."""

import base64
import binascii
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

_background_job_lock = threading.Lock()


@contextmanager
def background_job_slot(database_path, name):
    """Serialize expensive background jobs per deployment, across threads/processes.

    Web reads and Sheets writes do not take this lock. OS locks are released on
    process exit, so an interrupted deployment cannot leave a stale job lock.
    """
    logger = logging.getLogger(__name__)
    waiting = time.monotonic()
    logger.warning('background_job state=queued job=%s', name)
    with _background_job_lock:
        with open(os.fspath(database_path) + '.background-job.lock', 'a+b') as handle:
            handle.seek(0, os.SEEK_END)
            if not handle.tell():
                handle.write(b'0')
                handle.flush()
            if os.name == 'nt':
                import msvcrt
                while True:
                    try:
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        time.sleep(0.2)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            started = time.monotonic()
            logger.warning('background_job state=started job=%s wait_seconds=%.3f', name, started-waiting)
            try:
                yield
            finally:
                handle.seek(0)
                if os.name == 'nt':
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                logger.warning('background_job state=finished job=%s seconds=%.3f', name, time.monotonic()-started)


def data_path(filename):
    directory = Path(os.getenv("DATA_DIR") or BASE_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    return str(directory / filename)


def google_credentials(filename, *, scopes):
    """Prefer env credentials; retain service_account.json for local installs."""
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    encoded = os.getenv("GOOGLE_SERVICE_ACCOUNT_BASE64", "").strip()
    if raw_json or encoded:
        try:
            if not raw_json:
                raw_json = base64.b64decode(encoded, validate=True).decode("utf-8")
            info = json.loads(raw_json)
            if not isinstance(info, dict):
                raise ValueError("Service account JSON must be an object")
        except (ValueError, UnicodeError, binascii.Error):
            raise RuntimeError(
                "Invalid Google service account environment configuration. "
                "Use GOOGLE_SERVICE_ACCOUNT_BASE64 or GOOGLE_SERVICE_ACCOUNT_JSON."
            ) from None
        return Credentials.from_service_account_info(info, scopes=scopes)

    credential_file = Path(os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE") or filename)
    if not credential_file.is_absolute():
        credential_file = BASE_DIR / credential_file
    if not credential_file.is_file():
        raise RuntimeError(
            "Google credentials were not found. Set GOOGLE_SERVICE_ACCOUNT_BASE64 "
            "or GOOGLE_SERVICE_ACCOUNT_JSON in .env, or provide service_account.json."
        )
    return Credentials.from_service_account_file(str(credential_file), scopes=scopes)
