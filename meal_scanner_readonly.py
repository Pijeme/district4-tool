"""IOWO offline scanner: website -> master ONLY for event/paid-roster downloads.

No Google Sheets calls, no server-side meal sessions, claims, disputes, or audits.
The Android Master owns those records in its on-device SQLite database.

Integration, placed after event_password_matches and event_limit are defined:
    from meal_scanner_readonly import register_meal_scanner_api
    register_meal_scanner_api(app, get_db, event_tables,
                              event_password_matches, event_limit)

IMPORTANT: separately EXEMPT /api/v2/meal-scanner/ in app.py's existing
before_request() hook to prevent website Google Sheets sync on API requests.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
from datetime import datetime, timezone
from functools import wraps

from flask import Blueprint, jsonify, request
from werkzeug.exceptions import HTTPException

PREFIX = '/api/v2/meal-scanner'
UUID32 = re.compile(r'^[0-9a-f]{32}$')
DEVICE_ID = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
TOKEN = re.compile(r'^[0-9a-f]{64}$')
MAX_REQUEST_BYTES = 16 * 1024


def register_meal_scanner_api(app, get_db, event_tables, check_admin_password, rate_limit):
    """Register API on an existing Flask app; `rate_limit` must fail at its limit."""
    if rate_limit is None:
        raise ValueError('meal scanner requires website event_limit rate limiter')

    api = Blueprint('meal_scanner_readonly', __name__, url_prefix=PREFIX)

    def now_utc():
        return datetime.now(timezone.utc).isoformat()

    def respond(status=200, **fields):
        response = jsonify(fields)
        response.status_code = status
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['Pragma'] = 'no-cache'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    def ensure_tables():
        # Uses original app_v2.db connection. No registration rows changed.
        event_tables()  # Maintains existing event schema incl. deleted_at.
        db = get_db()
        db.execute('''CREATE TABLE IF NOT EXISTS meal_scanner_download_devices (
            device_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            secret_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            revoked_at TEXT NOT NULL DEFAULT ''
        )''')
        db.commit()
        return db

    def auth_rate_limit(action):
        # The existing website's persistent SQLite rate limiter. This is
        # called BEFORE any password check, for BOTH enroll and revoke.
        try:
            event_tables()  # ensures event_request_limits on fresh databases
            rate_limit('meal-scanner-' + action, 5, 900)
        except HTTPException as exc:
            if exc.code == 429:
                return respond(429, ok=False, code='RATE_LIMITED',
                               error='Too many authorization attempts. Try again later.')
            raise
        return None

    @api.before_request
    def validate_request_size():
        if request.content_length is not None and request.content_length > MAX_REQUEST_BYTES:
            return respond(413, ok=False, code='REQUEST_TOO_LARGE', error='Request too large')

    @api.errorhandler(HTTPException)
    def blueprint_http_error(exc):
        return respond(exc.code or 500, ok=False, code='HTTP_ERROR',
                       error=exc.description if (exc.code or 500) < 500 else 'Server error')

    @api.errorhandler(Exception)
    def blueprint_unexpected_error(exc):
        # Do not leak database errors or stack traces to the Android client.
        app.logger.exception('meal_scanner_api_failed: %s', type(exc).__name__)
        return respond(500, ok=False, code='INTERNAL_ERROR', error='Scanner API temporarily unavailable')

    def protected(view):
        @wraps(view)
        def check(*args, **kwargs):
            device_id = request.headers.get('X-IOWO-Device-ID', '').strip()
            header = request.headers.get('Authorization', '')
            if not DEVICE_ID.fullmatch(device_id) or not header.startswith('Bearer '):
                return respond(401, ok=False, code='NOT_AUTHORIZED',
                               error='Authorize this master device first')
            token = header[7:].strip()
            if not TOKEN.fullmatch(token):
                return respond(401, ok=False, code='INVALID_CREDENTIALS',
                               error='Invalid device credentials')

            db = ensure_tables()
            row = db.execute('''SELECT secret_hash,revoked_at
                                FROM meal_scanner_download_devices
                                WHERE device_id=?''', (device_id,)).fetchone()
            digest = hashlib.sha256(token.encode('ascii')).hexdigest()
            if not row or row['revoked_at'] or not hmac.compare_digest(digest, row['secret_hash']):
                return respond(401, ok=False, code='REVOKED_OR_EXPIRED',
                               error='Master device authorization expired or revoked')

            db.execute('''UPDATE meal_scanner_download_devices
                          SET last_seen=? WHERE device_id=?''', (now_utc(), device_id))
            db.commit()
            return view(*args, **kwargs)
        return check

    def read_json_object():
        # Flask accepts arrays as JSON; the API expressly does not.
        data = request.get_json(silent=True)
        return data if isinstance(data, dict) else None

    @api.post('/enroll')
    def enroll():
        data = read_json_object()
        if data is None:
            return respond(400, ok=False, code='INVALID_JSON', error='Expected JSON object')
        # Limit guesses even if the supplied device id is invalid.
        limit_error = auth_rate_limit('enrollment')
        if limit_error is not None:
            return limit_error
        device_id = data.get('device_id')
        label = data.get('label', 'Master Android')
        password = data.get('admin_password')
        if (not isinstance(device_id, str) or not DEVICE_ID.fullmatch(device_id)
                or not isinstance(label, str) or not label.strip() or len(label) > 64
                or not isinstance(password, str) or len(password) > 256):
            return respond(400, ok=False, code='INVALID_INPUT', error='Invalid device information')
        if not check_admin_password(password):
            return respond(403, ok=False, code='BAD_ADMIN_PASSWORD',
                           error='Invalid website event-admin password')
        db = ensure_tables()
        timestamp = now_utc()
        token = secrets.token_hex(32)
        db.execute('''INSERT INTO meal_scanner_download_devices
            (device_id,label,secret_hash,created_at,last_seen,revoked_at)
            VALUES(?,?,?,?,?,'')
            ON CONFLICT(device_id) DO UPDATE SET
            label=excluded.label,secret_hash=excluded.secret_hash,
            last_seen=excluded.last_seen,revoked_at='' ''',
            (device_id, label.strip(), hashlib.sha256(token.encode('ascii')).hexdigest(),
             timestamp, timestamp))
        db.commit()
        return respond(ok=True, device_id=device_id, access_token=token,
                       message='Master authorized; store token securely on Android')

    @api.get('/events')
    @protected
    def events():
        db = get_db()
        available = db.execute('''SELECT key,name,kind,start_date,end_date,active,updated_at
                                  FROM registration_events WHERE deleted_at=''
                                  ORDER BY id DESC''').fetchall()
        deleted = db.execute('''SELECT key FROM registration_events
                                WHERE deleted_at<>'' ''').fetchall()
        return respond(ok=True, events=[dict(r) for r in available],
                       deleted_event_keys=[r['key'] for r in deleted],
                       server_time=now_utc())

    @api.get('/events/<event_key>/paid-registrations')
    @protected
    def paid_registrations(event_key):
        if not UUID32.fullmatch(event_key):
            return respond(404, ok=False, code='EVENT_NOT_FOUND', error='Event not found')
        db = get_db()
        event = db.execute('''SELECT id,key,name,start_date,end_date,deleted_at
                              FROM registration_events WHERE key=?''', (event_key,)).fetchone()
        if not event:
            return respond(404, ok=False, code='EVENT_NOT_FOUND', error='Event not found')
        if event['deleted_at']:
            # Explicit tombstone so the Android app can mark its cached event
            # unavailable WITHOUT removing the local claims/audit history.
            return respond(410, ok=False, code='EVENT_DELETED',
                           error='Event was deleted on the website', event_key=event_key)

        rows = db.execute('''SELECT key,name,number,area,church,updated_at,revision
                             FROM event_attendees
                             WHERE event_id=? AND complete=1 AND paid=1 AND deleted_at=''
                             ORDER BY key''', (event['id'],)).fetchall()
        people = [dict(r) for r in rows]
        snapshot = hashlib.sha256(json.dumps(people, sort_keys=True, separators=(',', ':'),
                                             ensure_ascii=False).encode('utf-8')).hexdigest()
        if request.args.get('cursor', '') == snapshot:
            return respond(ok=True, changed=False, cursor=snapshot,
                           event_key=event_key, total=len(people), attendees=[],
                           server_time=now_utc())
        return respond(ok=True, changed=True, cursor=snapshot,
                       event_key=event_key, total=len(people), attendees=people,
                       server_time=now_utc())

    @api.post('/revoke')
    def revoke():
        data = read_json_object()
        if data is None:
            return respond(400, ok=False, code='INVALID_JSON', error='Expected JSON object')
        limit_error = auth_rate_limit('revocation')
        if limit_error is not None:
            return limit_error
        password = data.get('admin_password')
        device_id = data.get('device_id')
        if (not isinstance(password, str) or len(password) > 256
                or not isinstance(device_id, str) or not DEVICE_ID.fullmatch(device_id)):
            return respond(400, ok=False, code='INVALID_INPUT', error='Invalid revocation request')
        if not check_admin_password(password):
            return respond(403, ok=False, code='BAD_ADMIN_PASSWORD',
                           error='Invalid website event-admin password')
        db = ensure_tables()
        db.execute('''UPDATE meal_scanner_download_devices SET revoked_at=?
                      WHERE device_id=?''', (now_utc(), device_id))
        db.commit()
        return respond(ok=True, message='If authorized, the device has been revoked')

    # Ensure an unknown scanner URL or a 405 returns JSON rather than the HTML
    # page that caused the Android error "Value <!DOCTYPE ... cannot be converted".
    @app.errorhandler(HTTPException)
    def meal_scanner_or_regular_error(exc):
        if request.path == PREFIX or request.path.startswith(PREFIX + '/'):
            return respond(exc.code or 500, ok=False, code='HTTP_ERROR',
                           error=exc.description if (exc.code or 500) < 500 else 'Server error')
        return exc  # Preserve the website's HTML handling for all other routes.

    app.register_blueprint(api)
