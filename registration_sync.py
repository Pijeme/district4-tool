"""Authenticated registration exchange shared by the website and offline companion.

No Google access, automatic synchronization, or application imports live here.
Event/ticket UUID keys are identity. Original creation time controls chronology.
"""
from __future__ import annotations

import hashlib
import hmac
import io
import json
import re
import secrets
import sqlite3
from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from functools import wraps

from flask import Blueprint, abort, g, jsonify, request, send_file
from PIL import Image

PREFIX = '/event-registration/api/offline/v1'
UUID = re.compile(r'^[a-f0-9]{32}$')
ASSET = re.compile(r'^[a-f0-9]{64}\.png$')
EDIT_FIELDS = ('name', 'birthday', 'contact', 'area', 'church', 'address', 'pastor',
               'new_church', 'photo', 'paid', 'physical_printed', 'released', 'deleted_at')
IDENTITY_FIELDS = ('key', 'event_key', 'created_at')
EVENT_FIELDS = ('key', 'name', 'kind', 'start_date', 'end_date', 'theme', 'scripture',
                'background', 'id_design', 'active', 'created_at', 'updated_at', 'deleted_at')
TEXT_LIMITS = {'name': 150, 'contact': 60, 'area': 30, 'church': 200,
               'address': 300, 'pastor': 150}


def now():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def timestamp(value):
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError('Invalid creation timestamp.')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('Timestamps must include their timezone.')
    return parsed.astimezone(timezone.utc).isoformat()


def content(record):
    """Fields shared between peers; local sequence numbers are not identity."""
    return {field: record.get(field, '' if field == 'deleted_at' else None)
            for field in IDENTITY_FIELDS + EDIT_FIELDS}


def merge_record(base, local, remote):
    """Three-way field merge; conflicts are explicit, including deletion conflicts."""
    if remote is None:
        return dict(local), []
    if base is None:
        return (dict(remote), []) if content(local) == content(remote) else (None, ['existing_ticket'])
    for field in IDENTITY_FIELDS:
        if local.get(field) != base.get(field) or remote.get(field) != base.get(field):
            return None, [field]
    local_changes = [field for field in EDIT_FIELDS if local.get(field) != base.get(field)]
    remote_changes = [field for field in EDIT_FIELDS if remote.get(field) != base.get(field)]
    if remote.get('deleted_at') and not local.get('deleted_at'):
        return None, ['remote_deleted']
    if local.get('deleted_at') and remote_changes and content(local) != content(remote):
        return None, ['deletion_conflict']
    conflicts = [field for field in local_changes if field in remote_changes
                 and local.get(field) != remote.get(field)]
    if conflicts:
        return None, conflicts
    merged = dict(remote)
    merged.update({field: local.get(field) for field in local_changes})
    if (merged.get('released') and not (merged.get('paid') and merged.get('physical_printed'))
            and content(merged) != content(remote)):
        return None, ['release_requires_payment_and_physical_print']
    return merged, []


def initialize(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS registration_sync_clients (
            key TEXT PRIMARY KEY, label TEXT NOT NULL, token_hash TEXT NOT NULL,
            created_at TEXT NOT NULL, last_seen TEXT NOT NULL, revoked_at TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS registration_sync_receipts (
            client_key TEXT NOT NULL, operation_id TEXT NOT NULL, request_hash TEXT NOT NULL,
            response_json TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(client_key, operation_id));
        CREATE TABLE IF NOT EXISTS registration_sync_origins (
            ticket_key TEXT PRIMARY KEY, client_key TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS registration_sync_audit (
            id INTEGER PRIMARY KEY, client_key TEXT NOT NULL, action TEXT NOT NULL,
            event_key TEXT NOT NULL DEFAULT '', ticket_key TEXT NOT NULL DEFAULT '',
            occurred_at TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS registration_sync_tombstones (
            ticket_key TEXT PRIMARY KEY, event_key TEXT NOT NULL, deleted_at TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS registration_sync_keep_deletions
        BEFORE DELETE ON event_attendees WHEN OLD.complete=1 BEGIN
            INSERT OR REPLACE INTO registration_sync_tombstones(ticket_key,event_key,deleted_at)
            SELECT OLD.key,e.key,CASE WHEN OLD.deleted_at<>'' THEN OLD.deleted_at
                ELSE strftime('%Y-%m-%dT%H:%M:%f+00:00','now') END
            FROM registration_events e WHERE e.id=OLD.event_id;
        END;
    ''')
    db.commit()


def serialize(db, row, event_key=None):
    person = dict(row)
    if event_key is None:
        event_key = db.execute('SELECT key FROM registration_events WHERE id=?',
                               (person['event_id'],)).fetchone()[0]
    result = {field: person.get(field, '') for field in EDIT_FIELDS}
    result.update(key=person['key'], event_key=event_key,
                  created_at=timestamp(person['created_at']), updated_at=person['updated_at'],
                  number=person['number'], revision=person['revision'], complete=bool(person['complete']))
    origin = db.execute('SELECT client_key FROM registration_sync_origins WHERE ticket_key=?',
                        (person['key'],)).fetchone()
    result['origin'] = 'offline' if origin else 'online'
    return result


def validate_record(record, event_key, *, strict_status=True):
    if not isinstance(record, dict) or not UUID.fullmatch(str(record.get('key', ''))):
        raise ValueError('A registration must have its original 32-character ticket key.')
    if record.get('event_key') != event_key:
        raise ValueError('Registration belongs to a different event.')
    clean = {field: record.get(field, '') for field in EDIT_FIELDS}
    clean.update(key=record['key'], event_key=event_key, created_at=timestamp(record.get('created_at')))
    if datetime.fromisoformat(clean['created_at']) > datetime.now(timezone.utc) + timedelta(minutes=10):
        raise ValueError('Creation time is in the future. Check the laptop clock.')
    for field, limit in TEXT_LIMITS.items():
        value = clean[field]
        if not isinstance(value, str) or len(value) > limit or (field != 'contact' and not value.strip()):
            raise ValueError('Invalid ' + field + '.')
    if clean['area'].strip().isdigit() and int(clean['area'].strip()) == 0:
        raise ValueError('Area 0 is not a registration area.')
    born = date.fromisoformat(str(clean.get('birthday')))
    if born < date(1900, 1, 1) or born > (datetime.now(timezone.utc)+timedelta(hours=8)).date():
        raise ValueError('Invalid birthday.')
    for field in ('new_church', 'paid', 'physical_printed', 'released'):
        value = clean[field]
        if type(value) not in (bool, int) or value not in (0, 1):
            raise ValueError('Invalid ' + field + ' status.')
        clean[field] = int(value)
    if not ASSET.fullmatch(str(clean['photo'])):
        raise ValueError('A completed registration needs its original PNG photo.')
    clean['deleted_at'] = timestamp(clean['deleted_at']) if clean['deleted_at'] else ''
    if strict_status and clean['released'] and not (clean['paid'] and clean['physical_printed']):
        raise ValueError('Release requires payment and confirmed physical printing.')
    return clean


def register_registration_sync(app, *, get_db, ensure_events, asset_path, asset_write,
                               check_password, rate_limit, directory, queue_deletion,
                               record_lock=None, validate_print=None):
    api = Blueprint('registration_sync', __name__, url_prefix=PREFIX)

    def prepare():
        ensure_events()
        initialize(get_db())

    def audit(action, event_key='', ticket_key='', detail=''):
        get_db().execute('INSERT INTO registration_sync_audit(client_key,action,event_key,ticket_key,occurred_at,detail) VALUES (?,?,?,?,?,?)',
                         (getattr(g, 'registration_client', ''), action, event_key, ticket_key, now(), detail))

    def protected(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            client = request.headers.get('X-IOWO-Laptop-ID', '')
            header = request.headers.get('Authorization', '')
            if not UUID.fullmatch(client) or not header.startswith('Bearer '):
                return jsonify(ok=False, code='UNAUTHORIZED', error='Authorize this laptop first.'), 401
            token = header[7:]
            if not re.fullmatch(r'[a-f0-9]{64}', token):
                return jsonify(ok=False, code='UNAUTHORIZED', error='Invalid laptop credentials.'), 401
            prepare()
            row = get_db().execute('SELECT token_hash,revoked_at FROM registration_sync_clients WHERE key=?', (client,)).fetchone()
            if not row or row['revoked_at'] or not hmac.compare_digest(row['token_hash'], hashlib.sha256(token.encode()).hexdigest()):
                return jsonify(ok=False, code='UNAUTHORIZED', error='Laptop authorization was revoked or replaced.'), 401
            g.registration_client = client
            get_db().execute('UPDATE registration_sync_clients SET last_seen=? WHERE key=?', (now(), client))
            get_db().commit()
            return view(*args, **kwargs)
        return wrapped

    @api.before_request
    def limits():
        if request.content_length is not None and request.content_length > 13*1024*1024:
            return jsonify(ok=False, code='TOO_LARGE', error='Request exceeds 13 MiB.'), 413

    @api.after_request
    def private(response):
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @api.errorhandler(ValueError)
    def invalid(exc):
        get_db().rollback()
        return jsonify(ok=False, code='INVALID_REQUEST', error=str(exc)), 400

    @api.errorhandler(sqlite3.OperationalError)
    def busy(exc):
        get_db().rollback()
        app.logger.warning('Registration exchange database operation failed: %s', type(exc).__name__)
        return jsonify(ok=False, code='RETRY', error='Database is busy. Retry this record manually.'), 503

    @api.errorhandler(Exception)
    def failed(exc):
        from werkzeug.exceptions import HTTPException
        get_db().rollback()
        if isinstance(exc, HTTPException):
            return jsonify(ok=False, code='HTTP_ERROR', error=exc.description), exc.code
        app.logger.exception('Registration exchange failed')
        return jsonify(ok=False, code='SERVER_ERROR', error='Exchange was not confirmed. Keep the record pending.'), 500

    @api.post('/enroll')
    def enroll():
        prepare()
        rate_limit('offline-registration-enroll', 5, 900)
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            raise ValueError('Send a JSON object.')
        key, label, password = body.get('laptop_id'), body.get('label'), body.get('admin_password')
        if not UUID.fullmatch(str(key)) or not isinstance(label, str) or not 1 <= len(label.strip()) <= 80 or not isinstance(password, str) or len(password) > 256:
            raise ValueError('Invalid laptop enrollment.')
        if not check_password(password):
            return jsonify(ok=False, code='BAD_PASSWORD', error='Incorrect website event-admin password.'), 403
        token = secrets.token_hex(32)
        instant = now()
        get_db().execute('''INSERT INTO registration_sync_clients VALUES (?,?,?,?,?,'')
            ON CONFLICT(key) DO UPDATE SET label=excluded.label,token_hash=excluded.token_hash,
            last_seen=excluded.last_seen,revoked_at='' ''',
            (key, label.strip(), hashlib.sha256(token.encode()).hexdigest(), instant, instant))
        g.registration_client = key
        audit('enroll')
        get_db().commit()
        return jsonify(ok=True, protocol=1, laptop_id=key, access_token=token)

    @api.post('/revoke')
    def revoke():
        prepare()
        rate_limit('offline-registration-revoke', 5, 900)
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or not UUID.fullmatch(str(body.get('laptop_id'))):
            raise ValueError('Invalid laptop identifier.')
        password = body.get('admin_password')
        if not isinstance(password, str) or not check_password(password):
            return jsonify(ok=False, code='BAD_PASSWORD', error='Incorrect website event-admin password.'), 403
        get_db().execute('UPDATE registration_sync_clients SET revoked_at=? WHERE key=?', (now(), body['laptop_id']))
        g.registration_client = body['laptop_id']
        audit('revoke')
        get_db().commit()
        return jsonify(ok=True)

    @api.get('/events')
    @protected
    def events():
        rows = get_db().execute("SELECT * FROM registration_events WHERE deleted_at='' ORDER BY start_date DESC,key")
        return jsonify(ok=True, protocol=1, events=[{field: row[field] for field in EVENT_FIELDS} for row in rows])

    @api.get('/events/<event_key>/snapshot')
    @protected
    def snapshot(event_key):
        if not UUID.fullmatch(event_key):
            abort(404)
        db = get_db()
        db.execute('BEGIN')
        try:
            event = db.execute('SELECT * FROM registration_events WHERE key=?', (event_key,)).fetchone()
            if not event or event['deleted_at']:
                return jsonify(ok=False, code='EVENT_UNAVAILABLE', error='The event is unavailable; local records were not deleted.'), 409
            rows = db.execute('SELECT * FROM event_attendees WHERE event_id=? AND complete=1 ORDER BY created_at,key', (event['id'],)).fetchall()
            churches, areas = directory()
            tombstones = [dict(row) for row in db.execute('SELECT * FROM registration_sync_tombstones WHERE event_key=?', (event_key,))]
            result = dict(ok=True, protocol=1, event={field: event[field] for field in EVENT_FIELDS},
                          records=[serialize(db, row, event_key) for row in rows], tombstones=tombstones,
                          directory=dict(churches=churches, areas=areas), captured_at=now())
            result['snapshot_hash'] = digest(result)
        finally:
            db.rollback()
        audit('download_snapshot', event_key, detail=str(len(result['records']))+' registrations')
        db.commit()
        return jsonify(result)

    @api.get('/assets/<filename>')
    @protected
    def download_asset(filename):
        if not ASSET.fullmatch(filename):
            abort(404)
        referenced = get_db().execute('''SELECT 1 FROM registration_events WHERE background=?
            UNION ALL SELECT 1 FROM event_attendees WHERE photo=? AND complete=1 LIMIT 1''', (filename, filename)).fetchone()
        if not referenced:
            abort(404)
        path = asset_path(filename)
        if not path.is_file():
            abort(404, 'Source image is missing on the website. Keep local records and retry after recovery.')
        return send_file(path, mimetype='image/png', conditional=False)

    @api.post('/assets/<filename>')
    @protected
    def upload_asset(filename):
        if not ASSET.fullmatch(filename):
            raise ValueError('Invalid image identifier.')
        body = request.get_data(cache=False)
        if not body or len(body) > 12*1024*1024 or hashlib.sha256(body).hexdigest()+'.png' != filename:
            raise ValueError('Image checksum or size does not match.')
        with Image.open(io.BytesIO(body)) as image:
            if image.format != 'PNG' or image.width*image.height > 80_000_000:
                raise ValueError('Upload a valid PNG image.')
            image.verify()
        saved = asset_write(body)
        if saved != filename:
            raise ValueError('Image was not saved under its content identity.')
        return jsonify(ok=True, filename=saved, sha256=filename[:-4])

    @api.post('/events/<event_key>/records')
    @protected
    def upload_record(event_key):
        if not UUID.fullmatch(event_key):
            abort(404)
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or not UUID.fullmatch(str(body.get('operation_id', ''))):
            raise ValueError('A persistent operation ID is required.')
        local = validate_record(body.get('record'), event_key)
        base = body.get('base')
        if base is not None:
            base = validate_record(base, event_key, strict_status=False)
            if base['key'] != local['key']:
                raise ValueError('Baseline belongs to another registration.')
        lock = record_lock(local['key']) if record_lock else nullcontext()
        with lock:
            db = get_db()
            db.execute('BEGIN IMMEDIATE')
            try:
                request_hash = digest(body)
                previous = db.execute('SELECT * FROM registration_sync_receipts WHERE client_key=? AND operation_id=?',
                                       (g.registration_client, body['operation_id'])).fetchone()
                if previous:
                    db.rollback()
                    if previous['request_hash'] != request_hash:
                        return jsonify(ok=False, code='OPERATION_REUSED', error='Operation ID has different content.'), 409
                    return jsonify(json.loads(previous['response_json']))
                event = db.execute("SELECT * FROM registration_events WHERE key=? AND deleted_at=''", (event_key,)).fetchone()
                if not event:
                    db.rollback()
                    return jsonify(ok=False, code='EVENT_UNAVAILABLE', error='Prepare this existing event on the website before uploading.'), 409
                row = db.execute('SELECT * FROM event_attendees WHERE key=?', (local['key'],)).fetchone()
                if row and row['event_id'] != event['id']:
                    db.rollback()
                    return jsonify(ok=False, code='CONFLICT', fields=['event_key'], error='Ticket key belongs to another event.'), 409
                tombstone = db.execute('SELECT * FROM registration_sync_tombstones WHERE ticket_key=?', (local['key'],)).fetchone()
                if tombstone or (row and row['deleted_at']):
                    db.rollback()
                    return jsonify(ok=False, code='CONFLICT', fields=['remote_deleted'],
                                   remote=serialize(db, row, event_key) if row else dict(tombstone),
                                   error='Website deletion requires review. Registration was not recreated.'), 409
                remote = serialize(db, row, event_key) if row else None
                if row and not row['complete']:
                    db.rollback()
                    return jsonify(ok=False, code='CONFLICT', fields=['unfinished_registration'], error='Website draft already uses this ticket key.'), 409
                if base is not None and remote is None:
                    db.rollback()
                    return jsonify(ok=False, code='CONFLICT', fields=['remote_missing'], error='Previously synchronized registration is missing online.'), 409
                merged, conflicts = merge_record(base, local, remote)
                if conflicts:
                    db.rollback()
                    return jsonify(ok=False, code='CONFLICT', fields=conflicts, remote=remote,
                                   error='Registration changed on both systems. Review before uploading.'), 409
                if not asset_path(merged['photo']).is_file():
                    raise ValueError('Upload and verify the attendee photo first.')
                if validate_print:
                    validate_print(dict(event), merged)
                instant = now()
                if merged['deleted_at']:
                    if row:
                        queued = dict(row)
                        queued['revision'] += 1
                        queued['deleted_at'] = merged['deleted_at']
                        queue_deletion(queued)
                    acknowledged = dict(merged, revision=(row['revision']+1 if row else 1), complete=True)
                    action = 'delete'
                elif row:
                    changes = any(merged[field] != remote[field] for field in EDIT_FIELDS)
                    if changes:
                        db.execute('''UPDATE event_attendees SET name=?,birthday=?,contact=?,area=?,church=?,address=?,pastor=?,
                            new_church=?,photo=?,paid=?,physical_printed=?,released=?,deleted_at=?,printed=?,
                            revision=revision+1,updated_at=? WHERE key=?''',
                            tuple(merged[field] for field in EDIT_FIELDS)+(merged['physical_printed'], instant, local['key']))
                    acknowledged = serialize(db, db.execute('SELECT * FROM event_attendees WHERE key=?', (local['key'],)).fetchone(), event_key)
                    action = 'update' if changes else 'unchanged'
                else:
                    if merged['deleted_at']:
                        raise ValueError('Cannot create a deleted registration.')
                    number = event['next_number']
                    db.execute('UPDATE registration_events SET next_number=next_number+1 WHERE id=?', (event['id'],))
                    db.execute('''INSERT INTO event_attendees(key,event_id,number,name,birthday,contact,area,church,address,pastor,
                        new_church,created_at,updated_at,photo,complete,paid,printed,physical_printed,released)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)''',
                        (local['key'], event['id'], number)+tuple(merged[field] for field in
                        ('name','birthday','contact','area','church','address','pastor','new_church'))+
                        (merged['created_at'],instant,merged['photo'],merged['paid'],merged['physical_printed'],merged['physical_printed'],merged['released']))
                    db.execute('INSERT INTO registration_sync_origins VALUES (?,?,?)', (local['key'], g.registration_client, instant))
                    acknowledged = serialize(db, db.execute('SELECT * FROM event_attendees WHERE key=?', (local['key'],)).fetchone(), event_key)
                    action = 'insert'
                response = dict(ok=True, operation_id=body['operation_id'], action=action, record=acknowledged)
                db.execute('INSERT INTO registration_sync_receipts VALUES (?,?,?,?,?)',
                           (g.registration_client, body['operation_id'], request_hash, canonical(response), instant))
                audit(action, event_key, local['key'])
                db.commit()
                return jsonify(response)
            except Exception:
                db.rollback()
                raise

    app.register_blueprint(api)
