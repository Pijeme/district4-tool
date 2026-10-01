"""2027 pledges: public viewing, selfie submissions and moderator review.

Google is authoritative. SQLite holds only a replaceable public cache and
request throttles; photos and pending submissions are never stored locally.
"""
import base64
import hashlib
import hmac
import io
import json
import os
import re
import secrets
import sys
import time
import warnings
from decimal import Decimal, InvalidOperation

import requests
from PIL import Image, ImageOps, UnidentifiedImageError
from flask import abort, current_app, flash, jsonify, make_response, redirect, render_template, request, session, url_for


class PledgeError(Exception):
    pass


def _db():
    return (sys.modules.get('app') or sys.modules['__main__']).get_db()


def _tables():
    db = _db()
    db.execute('CREATE TABLE IF NOT EXISTS pt2027_cache (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, updated REAL NOT NULL, attempted REAL NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS pt2027_throttle (key TEXT PRIMARY KEY, started REAL NOT NULL, count INTEGER NOT NULL)')
    db.commit()


def configured():
    return bool(current_app.config['PLEDGE_SCRIPT_URL'] and len(current_app.config['PLEDGE_API_TOKEN']) >= 32)


def secure_session():
    return len(str(current_app.secret_key or '')) >= 32 and current_app.secret_key != 'change-this-secret-key-123'


def is_moderator():
    # Only set by successful password authentication, never by AO church selection.
    actor = str(session.get('pledge_actor') or '')
    return secure_session() and bool(actor) and hmac.compare_digest(actor, current_app.config['PLEDGE_MODERATOR'])


def _require_moderator():
    if not is_moderator():
        abort(403)


def _csrf():
    if 'pledge_csrf' not in session:
        session['pledge_csrf'] = secrets.token_urlsafe(32)
    return session['pledge_csrf']


def _verify_csrf():
    supplied = request.headers.get('X-CSRF-Token') or request.form.get('csrf_token', '')
    if not supplied or not hmac.compare_digest(supplied, session.get('pledge_csrf', '')):
        abort(400, 'This form expired. Reload the page and try again.')


def _bridge(action, **payload):
    if not configured():
        raise PledgeError('Pledge storage is not connected yet. Please try again after setup.')
    envelope = {'action': action, 'token': current_app.config['PLEDGE_API_TOKEN'], **payload}
    try:
        response = requests.post(current_app.config['PLEDGE_SCRIPT_URL'], json=envelope, timeout=(10, 65))
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError):
        current_app.logger.warning('Pledge bridge unavailable (action=%s)', action)
        raise PledgeError('Google storage did not confirm the request. Please retry; the same submission will not be added twice.')
    if not isinstance(data, dict) or not data.get('ok'):
        code = data.get('code', '') if isinstance(data, dict) else ''
        messages = {
            'CONFLICT': 'This pledge changed since you opened it. Reload the approval page and review again.',
            'INVALID': 'Please check the submitted fields and try again.',
            'BUSY': 'Another pledge is being processed. Please try again shortly.',
            'NOT_FOUND': 'This submission could not be found. Reload the page.',
            'AUTH': 'The pledge storage connection needs administrator setup.',
        }
        raise PledgeError(messages.get(code, 'Pledge storage could not complete this request. Please retry or contact the moderator.'))
    return data


def _submission_is_pending(submission_id):
    """Confirm an uncertain submit by checking Google's pending list.

    A submit may complete in Google Sheets/Drive even if the final HTTP
    acknowledgement is lost or misreported. The Apps Script submission ID is
    idempotent, so verifying the newest pending page lets us safely recognize
    that successful write without creating a duplicate.
    """
    try:
        data = _bridge('pending', page=1)
    except PledgeError:
        return False

    items = data.get('items', []) if isinstance(data, dict) else []
    return any(str(item.get('id', '')) == submission_id for item in items if isinstance(item, dict))


def cash_value(raw):
    text = str(raw or '').strip().replace('₱', '').replace(',', '').strip()
    if not text:
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise ValueError('Enter a valid cash amount.')
    if not value.is_finite() or value < 0 or value > Decimal('999999999.99') or value != value.quantize(Decimal('.01')):
        raise ValueError('Cash must be a positive amount with no more than two decimal places.')
    return value


def _public_data(force=False):
    _tables()
    db = _db()
    row = db.execute('SELECT * FROM pt2027_cache WHERE id=1').fetchone()
    now = time.time()
    if row and not force and now - row['attempted'] < 300:
        return json.loads(row['payload']), row['updated'], row['attempted'] != row['updated']
    try:
        data = _bridge('public')
        # Whitelist public fields; never persist links or private pending data here.
        records = []
        for item in data['records']:
            record = {key: str(item.get(key, '')) for key in ('area', 'church', 'name', 'cash', 'livestock', 'goods')}
            cash_value(record['cash'])  # Malformed cash must not silently become zero.
            records.append(record)
        accounts = [{k: str(a.get(k, '')) for k in ('key', 'area', 'church', 'name')} for a in data['accounts']]
        result = {'records': records, 'accounts': accounts}
        db.execute('INSERT OR REPLACE INTO pt2027_cache VALUES (1,?,?,?)', (json.dumps(result), now, now))
        db.commit()
        return result, now, False
    except (PledgeError, KeyError, TypeError, ValueError):
        if not row:
            raise PledgeError('Pledges could not be loaded yet. Please try again shortly.')
        db.execute('UPDATE pt2027_cache SET attempted=? WHERE id=1', (now,))
        db.commit()
        return json.loads(row['payload']), row['updated'], True


def _throttle():
    _tables()
    # Trust the actual peer, not arbitrary X-Forwarded-For values.
    identity = (request.remote_addr or '') + '|' + session.setdefault('pledge_visitor', secrets.token_hex(16))
    key = hashlib.sha256(identity.encode()).hexdigest()
    db = _db()
    now = time.time()
    db.execute('BEGIN IMMEDIATE')
    try:
        db.execute('DELETE FROM pt2027_throttle WHERE started < ?', (now-3600,))
        row = db.execute('SELECT count FROM pt2027_throttle WHERE key=?', (key,)).fetchone()
        if row and row['count'] >= 8:
            raise PledgeError('You have made several attempts. Please wait an hour before submitting again.')
        db.execute('INSERT INTO pt2027_throttle VALUES (?,?,1) ON CONFLICT(key) DO UPDATE SET count=count+1', (key, now))
        db.commit()
    except Exception:
        db.rollback()
        raise


def _selfie(upload):
    if not upload:
        raise ValueError('Please take your selfie before submitting.')
    raw = upload.read(5 * 1024 * 1024 + 1)
    if not raw or len(raw) > 5 * 1024 * 1024:
        raise ValueError('The selfie is too large. Please retake it.')
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error', Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as photo:
                if photo.format not in ('JPEG', 'PNG', 'WEBP') or photo.width * photo.height > 16000000:
                    raise ValueError('Please use a camera photo.')
                photo.load()
                photo = ImageOps.exif_transpose(photo).convert('RGB')
                photo.thumbnail((800, 800))
                out = io.BytesIO()
                photo.save(out, 'JPEG', quality=75, optimize=True)
        return base64.b64encode(out.getvalue()).decode('ascii')
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise ValueError('The selfie could not be read. Please retake it.')


def register_thanksgiving_pledges(app):
    app.config.setdefault('PLEDGE_SCRIPT_URL', os.getenv('PLEDGE_SCRIPT_URL', '').strip())
    app.config.setdefault('PLEDGE_API_TOKEN', os.getenv('PLEDGE_API_TOKEN', '').strip())
    app.config.setdefault('PLEDGE_MODERATOR', 'Pijeme')
    url = app.config['PLEDGE_SCRIPT_URL']
    if url and not re.fullmatch(r'https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec', url):
        raise RuntimeError('PLEDGE_SCRIPT_URL must be a deployed Google Apps Script /exec URL.')

    @app.route('/thanksgiving-pledges')
    def thanksgiving_pledges():
        error, updated, stale = None, None, False
        data = {'records': [], 'accounts': []}
        try:
            data, updated, stale = _public_data()
        except PledgeError as exc:
            error = str(exc)
        query = request.args.get('q', '').strip()[:100]
        area = request.args.get('area', '').strip()[:30]
        records = data['records']
        filtered = [r for r in records if (not area or r['area'] == area) and (not query or query.casefold() in (' '.join(r[k] for k in ('name', 'church', 'area'))).casefold())]
        total = sum((cash_value(r['cash']) or Decimal(0) for r in records), Decimal(0))
        subtotal = sum((cash_value(r['cash']) or Decimal(0) for r in filtered), Decimal(0))
        if 'pledge_form_id' not in session:
            session['pledge_form_id'] = secrets.token_hex(16)
        count = None
        if is_moderator() and configured():
            try:
                count = _bridge('count')['count']
            except PledgeError:
                pass
        return render_template('thanksgiving_pledges.html', records=filtered, accounts=data['accounts'], total=total,
            subtotal=subtotal, area=area, q=query, areas=sorted({r['area'] for r in records if r['area']}, key=lambda x: (len(x), x)),
            error=error, stale=stale, updated=updated, moderator=is_moderator(), pending_count=count,
            ready=configured() and secure_session(), csrf_token=_csrf(), submission_id=session['pledge_form_id'])

    @app.route('/thanksgiving-pledges/submit', methods=['POST'])
    def pledge_submit():
        # Limit only this route; do not change limits on existing website uploads.
        if request.content_length is None or request.content_length > 6 * 1024 * 1024:
            return jsonify(ok=False, error='The selfie is too large. Please retake it.'), 413
        _verify_csrf()
        if not configured() or not secure_session():
            return jsonify(ok=False, error='Pledge submissions are awaiting administrator setup.'), 503
        try:
            sid = request.form.get('submission_id', '')
            if not re.fullmatch('[a-f0-9]{32}', sid) or sid != session.get('pledge_form_id'):
                raise ValueError('This form expired. Reload the page and try again.')
            if request.form.get('website', ''):
                raise ValueError('Unable to submit this form.')
            kind = request.form.get('kind', '')
            cash = cash_value(request.form.get('cash', ''))
            livestock = request.form.get('livestock', '').strip()
            goods = request.form.get('goods', '').strip()
            if len(livestock) > 500 or len(goods) > 500:
                raise ValueError('Please keep each pledge description within 500 characters.')
            if not (cash and cash > 0) and not livestock and not goods:
                raise ValueError('Please enter at least one pledge.')
            payload = {'submission_id': sid, 'kind': kind, 'cash': str(cash) if cash is not None else '', 'livestock': livestock, 'goods': goods}
            if kind == 'Pastor':
                # The bridge re-resolves this opaque key against live Accounts data.
                payload['account_key'] = request.form.get('account_key', '')[:80]
            elif kind == 'Others':
                payload['name'] = request.form.get('name', '').strip()
                payload['church'] = request.form.get('church', '').strip()
                if not payload['name'] or not payload['church'] or len(payload['name']) > 150 or len(payload['church']) > 250:
                    raise ValueError('Please enter your name and church address within the indicated limits.')
            else:
                raise ValueError('Choose Pastor or Others.')
            payload['photo'] = _selfie(request.files.get('selfie'))
            _throttle()
            try:
                result = _bridge('submit', **payload)
            except PledgeError as exc:
                # Google may have completed the Sheet/Drive write even when the
                # final acknowledgement is lost or misreported. Confirm the same
                # idempotent submission ID before showing an error to the user.
                current_app.logger.warning(
                    'Pledge submit acknowledgement failed for %s; verifying pending storage',
                    sid,
                )
                if _submission_is_pending(sid):
                    current_app.logger.info(
                        'Pledge %s was confirmed in Google after acknowledgement failure',
                        sid,
                    )
                    return jsonify(ok=True, reference=sid, status='Pending', recovered=True)
                raise exc

            # Keep the ID stable until the browser receives success, then redirects.
            return jsonify(ok=True, reference=sid, status=result.get('status', 'Pending'))
        except ValueError as exc:
            return jsonify(ok=False, error=str(exc)), 400
        except PledgeError as exc:
            return jsonify(ok=False, error=str(exc)), 503

    @app.route('/thanksgiving-pledges/new-form', methods=['POST'])
    def pledge_new_form():
        _verify_csrf()
        session.pop('pledge_form_id', None)
        flash('Thank you! Your pledge is saved and awaiting moderator approval.', 'success')
        return redirect(url_for('thanksgiving_pledges'))

    @app.route('/thanksgiving-pledges/pending')
    def pledge_pending():
        _require_moderator()
        try:
            page = max(1, int(request.args.get('page', '1')))
        except ValueError:
            page = 1
        error, data = None, {'items': [], 'total': 0, 'page': page, 'pages': 1}
        try:
            data = _bridge('pending', page=page)
        except PledgeError as exc:
            error = str(exc)
        response = make_response(render_template('thanksgiving_pledges_admin.html', data=data, error=error, csrf_token=_csrf()))
        response.headers['Cache-Control'] = 'no-store'
        return response

    @app.route('/thanksgiving-pledges/decision', methods=['POST'])
    def pledge_decision():
        _require_moderator()
        _verify_csrf()
        try:
            result = _bridge('decide', submission_id=request.form.get('submission_id', ''), decision=request.form.get('decision', ''),
                target=request.form.get('target', ''), reviewer=session['pledge_actor'])
            _tables()
            _db().execute('UPDATE pt2027_cache SET attempted=0 WHERE id=1')
            _db().commit()
            flash('Pledge ' + result['status'].lower() + '.', 'success')
        except PledgeError as exc:
            flash(str(exc), 'error')
        return redirect(url_for('pledge_pending'))

    @app.route('/thanksgiving-pledges/selfie/<submission_id>')
    def pledge_selfie(submission_id):
        _require_moderator()
        if not re.fullmatch('[a-f0-9]{32}', submission_id):
            abort(404)
        try:
            data = _bridge('photo', submission_id=submission_id)
            body = base64.b64decode(data['photo'], validate=True)
        except (PledgeError, ValueError, KeyError):
            abort(503)
        response = make_response(body)
        response.headers['Content-Type'] = 'image/jpeg'
        response.headers['Cache-Control'] = 'no-store, private'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    @app.route('/thanksgiving-pledges/refresh', methods=['POST'])
    def pledge_refresh():
        _require_moderator()
        _verify_csrf()
        try:
            _, _, stale = _public_data(force=True)
            flash('Google is unavailable; showing the last saved list.' if stale else 'Pledges refreshed.', 'error' if stale else 'success')
        except PledgeError as exc:
            flash(str(exc), 'error')
        return redirect(url_for('thanksgiving_pledges'))
