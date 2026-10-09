"""Shared storage paths and Google credentials for local and container runs."""

import base64
import binascii
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


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
