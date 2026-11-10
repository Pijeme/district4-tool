"""IOWO scanner API: device enrollment, incremental paid roster and meal sync.

Uploads reuse the website's existing meal handlers and tables. Scanner requests
never trigger Google Sheets synchronization. The Android master approves offline.

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
import sqlite3
from datetime import datetime, timezone, timedelta
from functools import wraps

from flask import Blueprint, jsonify, request, g
from werkzeug.exceptions import HTTPException

PREFIX = '/api/v2/meal-scanner'
UUID32 = re.compile(r'^[0-9a-f]{32}$')
DEVICE_ID = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
TOKEN = re.compile(r'^[0-9a-f]{64}$')
MAX_REQUEST_BYTES = 512 * 1024


def register_meal_scanner_api(app, get_db, event_tables, check_admin_password, rate_limit,
                            meal_tables=None, meal_views=None):
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
        if 'authorized_at' not in {r['name'] for r in db.execute(
                'PRAGMA table_info(meal_scanner_download_devices)')}:
            db.execute("ALTER TABLE meal_scanner_download_devices ADD COLUMN authorized_at TEXT NOT NULL DEFAULT ''")
        db.execute('''CREATE TABLE IF NOT EXISTS meal_scanner_roster_snapshots (
            device_id TEXT NOT NULL, event_key TEXT NOT NULL, cursor TEXT NOT NULL,
            hashes_json TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(device_id,event_key,cursor))''')
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
        get_db().rollback()
        return respond(exc.code or 500, ok=False, code='HTTP_ERROR',
                       error=exc.description if (exc.code or 500) < 500 else 'Server error')

    @api.errorhandler(Exception)
    def blueprint_unexpected_error(exc):
        # Do not leak database errors or stack traces to the Android client.
        get_db().rollback()
        app.logger.exception('meal_scanner_api_failed: %s', type(exc).__name__)
        return respond(500, ok=False, code='INTERNAL_ERROR', error='Scanner API temporarily unavailable')

    @api.errorhandler(ValueError)
    def invalid_request(exc):
        get_db().rollback()
        return respond(400, ok=False, code='INVALID_REQUEST', error=str(exc))

    @api.errorhandler(sqlite3.IntegrityError)
    def conflicting_request(exc):
        get_db().rollback()
        return respond(409, ok=False, code='CONFLICT', error='A conflicting meal record already exists')

    @api.errorhandler(sqlite3.OperationalError)
    def busy_request(exc):
        get_db().rollback()
        app.logger.exception('meal_scanner_database_busy')
        return respond(503, ok=False, code='RETRY', error='Website database is busy. Please retry.')

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
            row = db.execute('''SELECT secret_hash,revoked_at,authorized_at
                                FROM meal_scanner_download_devices
                                WHERE device_id=?''', (device_id,)).fetchone()
            digest = hashlib.sha256(token.encode('ascii')).hexdigest()
            if not row or row['revoked_at'] or not hmac.compare_digest(digest, row['secret_hash']):
                return respond(401, ok=False, code='REVOKED_OR_EXPIRED',
                               error='Master device authorization expired or revoked')

            try:
                authorized_at = datetime.fromisoformat(row['authorized_at'])
                valid = (timedelta(0) <= datetime.now(timezone.utc) - authorized_at < timedelta(days=5))
            except (ValueError, TypeError):
                valid = False
            if not valid:
                return respond(401, ok=False, code='LOGIN_EXPIRED',
                               error='Five-day login expired. Log in with both passwords again.')
            g.event_master_id = device_id
            g.meal_scanner_device_id = device_id

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
            (device_id,label,secret_hash,created_at,last_seen,revoked_at,authorized_at)
            VALUES(?,?,?,?,?,'',?)
            ON CONFLICT(device_id) DO UPDATE SET
            label=excluded.label,secret_hash=excluded.secret_hash,
            last_seen=excluded.last_seen,revoked_at='',authorized_at=excluded.authorized_at ''',
            (device_id, label.strip(), hashlib.sha256(token.encode('ascii')).hexdigest(),
             timestamp, timestamp, timestamp))
        db.commit()
        return respond(ok=True, device_id=device_id, access_token=token,
                       expires_at=(datetime.now(timezone.utc)+timedelta(days=5)).isoformat(),
                       message='Master authorized for five days; store token securely on Android')

    @api.get('/events')
    @protected
    def events():
        db = get_db()
        available = db.execute('''SELECT key,name,kind,start_date,end_date,active,updated_at,theme,scripture
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
        current_hashes = {p['key']: hashlib.sha256(json.dumps(p, sort_keys=True,
            separators=(',', ':'), ensure_ascii=False).encode()).hexdigest() for p in people}
        cursor = request.args.get('cursor', '')
        baseline = db.execute('''SELECT hashes_json FROM meal_scanner_roster_snapshots
            WHERE device_id=? AND event_key=? AND cursor=?''',
            (g.meal_scanner_device_id, event_key, cursor)).fetchone() if cursor else None
        old_hashes = json.loads(baseline['hashes_json']) if baseline else {}
        mode = 'delta' if baseline or cursor == snapshot else 'full'
        changed_people = [] if cursor == snapshot else [p for p in people
            if mode == 'full' or old_hashes.get(p['key']) != current_hashes[p['key']]]
        removed = sorted(set(old_hashes) - set(current_hashes)) if mode == 'delta' else []
        db.execute('''INSERT INTO meal_scanner_roster_snapshots VALUES(?,?,?,?,?)
            ON CONFLICT(device_id,event_key,cursor) DO UPDATE SET
            hashes_json=excluded.hashes_json,created_at=excluded.created_at''',
            (g.meal_scanner_device_id, event_key, snapshot, json.dumps(current_hashes), now_utc()))
        # Retain the two previous baselines so a response lost in transit is retryable.
        db.execute('''DELETE FROM meal_scanner_roster_snapshots WHERE device_id=? AND event_key=?
            AND cursor NOT IN (SELECT cursor FROM meal_scanner_roster_snapshots
                WHERE device_id=? AND event_key=? ORDER BY created_at DESC LIMIT 3)''',
            (g.meal_scanner_device_id,event_key,g.meal_scanner_device_id,event_key))
        db.commit()
        return respond(ok=True, changed=cursor != snapshot, cursor=snapshot, mode=mode,
                       event_key=event_key, total=len(people), attendees=changed_people,
                       removed_keys=removed, server_time=now_utc())

    # Both APIs use the SAME event_meal_* tables and business rules. The scanner
    # authenticates with its enrolled per-device token rather than a shared token.
    for suffix, view in (meal_views or {}).items():
        def register_upload(suffix=suffix, view=view):
            @protected
            def upload(event_key):
                meal_tables()
                g.scanner_historical_upload = True
                return view(event_key)
            api.add_url_rule('/events/<event_key>/' + suffix,
                endpoint='sync_' + suffix.replace('-', '_'), view_func=upload,
                methods=['GET'] if suffix == 'sync-status' else ['POST'])
        register_upload()

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
