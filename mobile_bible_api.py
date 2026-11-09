"""Optional IOWO website bridge. Install only after current backups finish.

No server credentials are included in the Android app. Device tokens authorize
only this account's Bible Study notes and backup operations.
"""
from contextlib import closing
import hashlib
import hmac
import importlib
import json
import re
import secrets
import sqlite3
import sys
import threading
import time
from pathlib import Path
from flask import Blueprint, g, jsonify, request


def register_mobile_bible_routes(app, owner=None, study=None):
    owner = owner or sys.modules.get(app.import_name)
    if owner is None or not hasattr(owner, 'get_db'):
        owner = importlib.import_module('app')
    study = study or importlib.import_module('bible_study')
    api = Blueprint('bible_mobile', __name__, url_prefix='/api/v1/bible-mobile')
    location = Path(app.config.get('BIBLE_MOBILE_AUTH_DATABASE') or
                    Path(app.config['BIBLE_SERMON_NOTES_DATABASE']).with_name('mobile_devices.db')).resolve()
    if Path(app.static_folder).resolve() in location.parents:
        raise ValueError('Mobile authentication storage must stay outside static assets.')
    secret = app.secret_key
    if not secret or secret == 'change-this-secret-key-123':
        raise ValueError('Configure website session security before enabling mobile login.')
    secret = secret.encode() if isinstance(secret, str) else secret
    db_lock = threading.RLock()

    def database():
        location.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(location, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.executescript('''CREATE TABLE IF NOT EXISTS devices (
            token_hash TEXT PRIMARY KEY,user_key TEXT NOT NULL,username TEXT NOT NULL,role TEXT NOT NULL,
            credential_hash TEXT NOT NULL,name TEXT NOT NULL,can_convert INTEGER NOT NULL,
            created REAL NOT NULL,last_used REAL NOT NULL,revoked INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS login_attempts (key TEXT PRIMARY KEY,count INTEGER NOT NULL,updated REAL NOT NULL);''')
        connection.commit()
        return closing(connection)

    def account(username):
        db = owner.get_db()
        for table, member in [('sheet_accounts_cache', False), ('sheet_members_account_cache', True)]:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone(): continue
            rows = db.execute('SELECT * FROM ' + table + ' WHERE LOWER(TRIM(username))=LOWER(?)', (username.strip(),)).fetchall()
            if len(rows) > 1: return None
            if not rows: continue
            row = dict(rows[0])
            role = 'member' if member else str(row.get('position') or '').strip().lower()
            # Preserve the same role prefixes used by current_reader_user_key
            # on the website, including existing AO/DO aliases. Changing a
            # prefix would silently connect a different Drive notes library.
            if role not in ('pastor', 'member', 'area overseer', 'ao', 'sub area overseer', 'district overseer', 'do'):
                return None
            name = str(row.get('name') or row['username'])
            canonical = str(row['username']).strip().lower()
            password = str(row.get('password') or '').strip()
            if not password: return None
            return dict(username=canonical, role=role, user_key=role+':'+canonical, name=name, password=password,
                        can_convert=False)
        return None

    def credential_hash(profile):
        return hmac.new(secret, (profile['user_key']+'\0'+profile['password']).encode(), hashlib.sha256).hexdigest()

    @api.before_request
    def authenticate():
        if request.method != 'POST' or request.mimetype != 'application/json':
            return jsonify(ok=False, error='Use a JSON POST request.'), 415
        if request.content_length is None or request.content_length > 600 * 1024:
            return jsonify(ok=False, error='The request is too large.'), 413
        if not isinstance(request.get_json(silent=True), dict):
            return jsonify(ok=False, error='A JSON object is required.'), 400
        if request.endpoint == 'bible_mobile.login': return None
        header = request.headers.get('Authorization', '')
        if not header.startswith('Bearer '): return jsonify(ok=False, error='Device login required.'), 401
        token = header[7:]
        if not re.fullmatch(r'[A-Za-z0-9_-]{32,128}', token): return jsonify(ok=False, error='Device login required.'), 401
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        with db_lock, database() as db:
            stored = db.execute('SELECT * FROM devices WHERE token_hash=? AND revoked=0', (token_hash,)).fetchone()
        profile = account(stored['username']) if stored else None
        if not profile or profile['user_key'] != stored['user_key'] or not hmac.compare_digest(credential_hash(profile), stored['credential_hash']):
            return jsonify(ok=False, error='Sign in again to reconnect this device.'), 401
        g.mobile_profile, g.mobile_token_hash = profile, token_hash
        with db_lock, database() as db:
            db.execute('UPDATE devices SET last_used=? WHERE token_hash=?', (time.time(), token_hash)); db.commit()

    @api.after_request
    def private(response):
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @api.post('/login')
    def login():
        payload = request.get_json()
        username, password = payload.get('username', ''), payload.get('password', '')
        if not isinstance(username, str) or not isinstance(password, str) or not username.strip() or len(username) > 200 or len(password) > 500:
            return jsonify(ok=False, error='Enter your website username and password.'), 400
        attempt_key = hashlib.sha256(((request.remote_addr or '')+'|'+username.strip().lower()).encode()).hexdigest()
        now = time.time()
        with db_lock, database() as db:
            failed = db.execute('SELECT count,updated FROM login_attempts WHERE key=?', (attempt_key,)).fetchone()
            if failed and failed['count'] >= 8 and now-failed['updated'] < 600:
                return jsonify(ok=False, error='Please wait a few minutes before trying this account again.'), 429
        owner.sync_from_sheets_if_needed()
        profile = account(username)
        if not profile or not hmac.compare_digest(profile['password'].encode(), password.strip().encode()):
            with db_lock, database() as db:
                count = failed['count']+1 if failed and now-failed['updated'] < 600 else 1
                db.execute('INSERT OR REPLACE INTO login_attempts VALUES (?,?,?)', (attempt_key, count, now)); db.commit()
            return jsonify(ok=False, error='Invalid username or password.'), 401
        token = secrets.token_urlsafe(32)
        with db_lock, database() as db:
            db.execute('INSERT INTO devices VALUES (?,?,?,?,?,?,?,?,?,0)',
                       (hashlib.sha256(token.encode()).hexdigest(), profile['user_key'], profile['username'], profile['role'],
                        credential_hash(profile), profile['name'], int(profile['can_convert']), now, now))
            db.execute('DELETE FROM login_attempts WHERE key=?', (attempt_key,)); db.commit()
        return jsonify(ok=True, profile=dict(token=token, **{key: profile[key] for key in ('user_key','username','role','name','can_convert')}))

    @api.post('/logout')
    def logout():
        with db_lock, database() as db:
            db.execute('UPDATE devices SET revoked=1 WHERE token_hash=?', (g.mobile_token_hash,)); db.commit()
        return jsonify(ok=True)

    @api.post('/drive')
    def drive():
        payload = dict(request.get_json())
        action = payload.pop('action', '')
        allowed = {'after','limit','index_version','note_id','version','base_version','request_id','note'}
        if action not in ('list','get','put','delete') or set(payload)-allowed:
            return jsonify(ok=False, error='Choose a supported notes operation.'), 400
        try:
            # The account key always comes from the verified token, never a
            # user_key supplied by the phone. Shared Drive credentials stay here.
            return jsonify(study.sermon_notes_remote(app, g.mobile_profile['user_key'], action, **payload))
        except ValueError as error:
            return jsonify(ok=False, error=str(error)), 503

    def job_key():
        return (str(app.config['BIBLE_SERMON_NOTES_DATABASE']), g.mobile_profile['user_key'])

    def job_state(key):
        with study._notes_jobs_lock:
            return dict(study._notes_jobs.get(key, dict(running=False, message='')))

    @api.post('/website-sync/start')
    def website_sync():
        key = job_key()
        with study._notes_jobs_lock:
            current = study._notes_jobs.get(key, {})
            if current.get('running'):
                if current.get('kind') != 'sync':
                    return jsonify(ok=False, error='Another notes operation is running. Let it finish before starting another backup.'), 409
                return jsonify(ok=True, job=dict(current))
            study._notes_jobs[key] = dict(running=True, kind='sync', message='Preparing website notes', completed=0, total=0)
        def progress(**values):
            with study._notes_jobs_lock: study._notes_jobs[key].update(values)
        def run():
            try:
                with app.app_context(): result = study.sync_sermon_notes(app, key[1], progress)
                progress(running=False, **result)
            except Exception as error:
                message = str(error) if isinstance(error, ValueError) else 'The website backup stopped. Its saved notes are safe; retry.'
                progress(running=False, error=message, message=message)
        if app.config.get('BIBLE_MOBILE_JOBS_INLINE'): run()
        else: threading.Thread(target=run, name='mobile-website-sync', daemon=True).start()
        return jsonify(ok=True, job=job_state(key))

    @api.post('/website-sync/status')
    def sync_status():
        return jsonify(ok=True, job=job_state(job_key()))

    app.register_blueprint(api)
