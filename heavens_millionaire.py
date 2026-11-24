"""Bible quiz: isolated SQLite state, local portraits and server-authoritative rules.

The only connection to ministry data is a read-only directory/account lookup.
No Google clients, ministry writes or process-local active-game state live here.
"""
import csv
import hashlib
import hmac
import io
import json
import math
import re
import secrets
import sqlite3
import time
import unicodedata
import warnings
from collections import Counter
from contextlib import closing, contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Blueprint, Response, current_app, g, jsonify, make_response, render_template, request, send_file, session
from PIL import Image, ImageOps, UnidentifiedImageError
from werkzeug.security import check_password_hash, generate_password_hash

from runtime_config import data_path

POINTS = (100, 200, 300, 500, 1000, 2000, 4000, 8000, 16000, 32000,
          64000, 125000, 250000, 500000, 1000000)
STAGES = ('Beginner',) * 5 + ('Intermediate',) * 2 + ('Advanced',) * 3 + ('Expert',) * 3 + ('Master',) * 2
SECONDS = (30,) * 5 + (45,) * 5 + (60,) * 5
DEFAULT_POLICY = dict(offers_enabled=True, offer_questions=[6, 11, 14], bonus_points=1000,
                      minimum_points=1000, maximum_offer=1000000)
RNG = secrets.SystemRandom()
game = Blueprint('heavens_millionaire', __name__, url_prefix='/bible-games')


class GameError(Exception):
    def __init__(self, message, status=400):
        self.message, self.status = message, status


def now():
    return current_app.config.get('HEAVENS_MILLIONAIRE_CLOCK', time.time)()


def db():
    if 'millionaire_db' not in g:
        conn = sqlite3.connect(current_app.config['HEAVENS_MILLIONAIRE_DATABASE'], timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA busy_timeout=10000')
        g.millionaire_db = conn
    return g.millionaire_db


@contextmanager
def transaction():
    conn = db()
    conn.execute('BEGIN IMMEDIATE')
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def local_db():
    return current_app.extensions['heavens_millionaire']['local_db']()


def area_overseer_entry(area):
    label = 'Area Overseer'
    key = hashlib.sha256((area.casefold() + '\0' + label.casefold()).encode()).hexdigest()[:24]
    return dict(id=key, area=area, church=label, pastor='', address='')


def directory():
    rows = local_db().execute("""SELECT name, age, sex, church_address, position FROM sheet_accounts_cache
        WHERE lower(trim(position)) IN ('pastor','area overseer') ORDER BY age,sex,name""").fetchall()
    churches = {}
    areas = set()
    for row in rows:
        area = str(row['age'] or '').strip()
        if not area:
            continue
        areas.add(area)
        if row['position'].strip().casefold() != 'pastor':
            continue
        church = str(row['sex'] or row['church_address'] or '').strip()
        if not church:
            continue
        key = hashlib.sha256((area.casefold() + '\0' + church.casefold()).encode()).hexdigest()[:24]
        entry = churches.setdefault(key, dict(id=key, area=area, church=church, pastors=[], address=str(row['church_address'] or '')))
        pastor = str(row['name'] or '').strip()
        if pastor and pastor not in entry['pastors']:
            entry['pastors'].append(pastor)
    for entry in churches.values():
        entry['pastor'] = ' / '.join(entry.pop('pastors'))
    for area in areas:
        entry = area_overseer_entry(area)
        churches[entry['id']] = entry
    return sorted(churches.values(), key=lambda c: (c['area'], c['church'] == 'Area Overseer', c['church']))


def site_account():
    """Verify a signed website identity against its current local account mirror."""
    if session.get('ao_logged_in') is True:
        username, kind = session.get('ao_username'), 'staff'
    elif session.get('pastor_logged_in') is True:
        username, kind = session.get('pastor_username'), 'staff'
    elif session.get('member_logged_in') is True:
        username, kind = session.get('username'), 'member'
    else:
        return None
    if not isinstance(username, str) or not username.strip():
        return None
    row = None
    if kind == 'member':
        row = local_db().execute('''SELECT username,name,area_number AS area,
            COALESCE(NULLIF(trim(church_id),''),trim(church_address)) AS church,pastor,
            'member' AS role FROM sheet_members_account_cache WHERE username=?''', (username,)).fetchone()
    if row is None:
        row = local_db().execute('''SELECT username,name,age AS area,
            COALESCE(NULLIF(trim(sex),''),trim(church_address)) AS church,position AS role
            FROM sheet_accounts_cache WHERE username=?''', (username,)).fetchone()
    return dict(row, key=kind + ':' + row['username'].casefold()) if row else None


def account_church(identity):
    """Resolve a linked player's affiliation from the verified local account only."""
    if not identity:
        raise GameError('Please sign in through the District website first.', 401)
    area = str(identity['area'] or '').strip()
    church_name = str(identity['church'] or '').strip()
    if area and identity['role'].strip().casefold() == 'area overseer':
        return area_overseer_entry(area)
    if not area or not church_name:
        raise GameError('Your website account is missing its Area or church. Please ask your administrator to update that account.', 409)
    match = next((c for c in directory() if c['area'].casefold() == area.casefold()
                  and church_name.casefold() in (c['church'].casefold(), c['address'].casefold())), None)
    if match:
        return match
    # An account can precede its pastoral-directory row. Keep its authoritative
    # affiliation and recorded pastor rather than asking the player to choose.
    return dict(id=hashlib.sha256((area.casefold() + '\0' + church_name.casefold()).encode()).hexdigest()[:24],
                area=area, church=church_name,
                pastor=identity.get('pastor') or (identity['name'] if identity['role'].strip().casefold() == 'pastor' else ''))


def administrator():
    identity = site_account()
    if identity and (identity['role'].strip().casefold() == 'district overseer' or
                     identity['username'].casefold() == 'pijeme' and identity['role'].strip().casefold() == 'area overseer'):
        return identity
    return None


def player(required=True):
    ident = session.get('hm_player')
    row = db().execute('''SELECT p.*,c.auth_version FROM game_players p JOIN game_player_credentials c
        ON c.player_id=p.id WHERE p.id=?''', (ident,)).fetchone() if ident else None
    if row and row['site_key']:
        identity = site_account()
        if not identity or identity['key'] != row['site_key']:
            row = None
    if row and row['auth_version'] != session.get('hm_auth_version'):
        row = None
    if required and row is None:
        raise GameError('Please sign in to your game account.', 401)
    return row


def sign_in(row):
    session['hm_player'] = row['id']
    session['hm_auth_version'] = db().execute('SELECT auth_version FROM game_player_credentials WHERE player_id=?', (row['id'],)).fetchone()[0]
    session['hm_csrf'] = secrets.token_urlsafe(32)
    session.permanent = True


def csrf():
    if not session.get('hm_csrf'):
        session['hm_csrf'] = secrets.token_urlsafe(32)
    return session['hm_csrf']


def body():
    if request.is_json:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            raise GameError('Send a JSON object with the request fields.')
        return payload
    return request.form


def audit(action, actor, details=None):
    db().execute('INSERT INTO game_audit_logs(created_at,actor,action,details) VALUES(?,?,?,?)',
                 (now(), actor, action, json.dumps(details or {})))


def rate_limit(label, maximum, period=900, identity=''):
    # Commit counters independently of failed authentication/game transactions.
    key = hashlib.sha256((label + ':' + str(request.remote_addr) + ':' + identity).encode()).hexdigest()
    with transaction() as conn:
        row = conn.execute('SELECT * FROM game_rate_limits WHERE bucket=?', (key,)).fetchone()
        if row and now() - row['started_at'] < period:
            if row['count'] >= maximum:
                raise GameError('Too many attempts. Please try again later.', 429)
            conn.execute('UPDATE game_rate_limits SET count=count+1 WHERE bucket=?', (key,))
        else:
            conn.execute('INSERT OR REPLACE INTO game_rate_limits VALUES(?,?,1)', (key, now()))
        conn.execute('DELETE FROM game_rate_limits WHERE started_at<?', (now() - 86400,))


def policy():
    row = db().execute("SELECT value FROM game_settings WHERE key='policy'").fetchone()
    return json.loads(row[0]) if row else DEFAULT_POLICY.copy()


def portrait(upload):
    if not upload:
        raise GameError('Please take or select your player portrait.')
    raw = upload.read(5 * 1024 * 1024 + 1)
    if len(raw) > 5 * 1024 * 1024:
        raise GameError('Please choose a photo smaller than 5 MB.', 413)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error', Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as opened:
                if opened.format not in ('JPEG', 'PNG', 'WEBP') or getattr(opened, 'n_frames', 1) != 1:
                    raise GameError('Please use a still JPEG, PNG or WebP photo.')
                if opened.width * opened.height > 25000000 or min(opened.size) < 80:
                    raise GameError('Please use a clear photo between 80 pixels and 25 megapixels.')
                image = ImageOps.fit(ImageOps.exif_transpose(opened).convert('RGB'), (320, 320), method=Image.Resampling.LANCZOS)
                for quality in (85, 75, 65, 55, 45):
                    encoded = io.BytesIO()
                    image.save(encoded, format='JPEG', quality=quality, optimize=True)
                    if encoded.tell() <= 60 * 1024:
                        break
                if encoded.tell() > 60 * 1024:
                    raise GameError('This photo could not be compressed. Please choose another.')
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombWarning, Image.DecompressionBombError):
        raise GameError('That file could not be read as a safe photo.') from None
    folder = Path(current_app.config['HEAVENS_MILLIONAIRE_PHOTOS'])
    folder.mkdir(parents=True, exist_ok=True)
    name = secrets.token_hex(24) + '.jpg'
    (folder / name).write_bytes(encoded.getvalue())
    return name


def delete_portrait(name):
    if name and re.fullmatch(r'[a-f0-9]{48}\.jpg', name):
        (Path(current_app.config['HEAVENS_MILLIONAIRE_PHOTOS']) / name).unlink(missing_ok=True)


def public_player(row):
    if row is None:
        return None
    return dict(id=row['id'], player_code=row['player_code'], name=row['name'], area=row['area'], church=row['church'],
                pastor=row['pastor'], photo=bool(row['photo']), photo_url=f"/bible-games/photo/{row['id']}?v={row['photo']}",
                created_at=row['created_at'], linked=bool(row['site_key']))


def round_row(attempt):
    return db().execute('SELECT * FROM game_answers WHERE session_id=? AND position=?', (attempt['id'], attempt['position'])).fetchone()


def finish(attempt, outcome, points):
    db().execute("UPDATE game_sessions SET status=?,final_points=?,finished_at=? WHERE id=? AND status='active'",
                 (outcome, points, now(), attempt['id']))


def resume_question(question):
    """Resume once from the server's saved remaining time, including after reload."""
    started = question['pause_started_at']
    if started is not None:
        db().execute('''UPDATE game_answers SET deadline=deadline+?,pause_started_at=NULL,pause_until=NULL
            WHERE id=? AND pause_started_at IS NOT NULL''', (max(0.0, now() - started), question['id']))


def expire(attempt):
    if attempt and attempt['status'] == 'active':
        question = round_row(attempt)
        if (question['outcome'] is None and question['pause_started_at'] is None
                and question['deadline'] is not None and now() >= question['deadline']):
            db().execute("UPDATE game_answers SET outcome='timeout',resolved_at=? WHERE id=?", (now(), question['id']))
            finish(attempt, 'timeout', attempt['guaranteed'])
            return db().execute('SELECT * FROM game_sessions WHERE id=?', (attempt['id'],)).fetchone()
    return attempt


def sweep():
    rows = db().execute("""SELECT s.* FROM game_sessions s JOIN game_answers a
        ON a.session_id=s.id AND a.position=s.position WHERE s.status='active'
        AND a.outcome IS NULL AND a.pause_started_at IS NULL AND a.deadline<=?""", (now(),)).fetchall()
    for row in rows:
        expire(row)


def attempt_for(owner, attempt_id=None):
    if attempt_id:
        row = db().execute('SELECT * FROM game_sessions WHERE id=? AND player_id=?', (attempt_id, owner['id'])).fetchone()
    else:
        row = db().execute("SELECT * FROM game_sessions WHERE player_id=? AND status='active'", (owner['id'],)).fetchone()
    if not row:
        raise GameError('This game was not found.', 404)
    return expire(row)


def state(attempt):
    question = round_row(attempt)
    payload = json.loads(question['question_json'])
    paused = attempt['status'] == 'active' and question['outcome'] is None and question['pause_started_at'] is not None
    result = dict(id=attempt['id'], status=attempt['status'], position=attempt['position'], earned=attempt['earned'],
                  guaranteed=attempt['guaranteed'], final_points=attempt['final_points'], interruptions=attempt['interruptions'],
                  server_time=now(), stage=STAGES[attempt['position'] - 1], total_seconds=SECONDS[attempt['position'] - 1],
                  token=question['token'], question=question_prompt(payload), options=json.loads(question['choices_json']),
                  deadline=question['deadline'], pause_until=question['pause_until'], outcome=question['outcome'],
                  paused=paused, paused_remaining=max(0.0, question['deadline']-question['pause_started_at']) if paused else None,
                  removed=json.loads(question['removed_json']),
                  lifelines=[r[0] for r in db().execute('SELECT kind FROM game_lifeline_usage WHERE session_id=?', (attempt['id'],))],
                  results=[dict(r) for r in db().execute('SELECT position,outcome FROM game_answers WHERE session_id=? AND outcome IS NOT NULL ORDER BY position', (attempt['id'],))])
    if question['outcome'] is not None or attempt['status'] != 'active':
        result.update(correct_choice=question['correct_choice'], explanation=payload.get('explanation', ''),
                      source=source_citation(payload), selected=question['answer_choice'])
    usage = db().execute('SELECT kind,result_json FROM game_lifeline_usage WHERE session_id=? AND position=? ORDER BY id',
                         (attempt['id'], attempt['position'])).fetchall()
    result['lifeline_results'] = {r['kind']: json.loads(r['result_json']) for r in usage}
    result['paused_lifeline'] = usage[-1]['kind'] if paused and usage else None
    offer = db().execute('SELECT id,total,status FROM game_offers WHERE session_id=? AND position=?',
                         (attempt['id'], attempt['position'])).fetchone()
    rules = json.loads(attempt['policy_json'])
    result['offer_available'] = (rules['offers_enabled'] and attempt['position'] in rules['offer_questions']
                                 and attempt['earned'] >= rules['minimum_points'] and question['outcome'] is None)
    result['offer'] = dict(offer) if offer else None
    return result


def question_prompt(payload):
    """Hide source attributions in legacy prompts; explanations retain citations."""
    text = payload['question']
    text = re.sub(r'^In the NIV Thompson Bible, which book contains', 'Which Bible book contains', text)
    text = re.sub(r'^Which topic is treated in Bruce Shelley[’\']s chapter titled (.+)\?$',
                  r'Which topic best matches the historical heading \1?', text)
    text = re.sub(r'^Under which historical era does Shelley place his chapter (.+)\?$',
                  r'Which historical era is associated with the heading \1?', text)
    text = re.sub(r'^Which of these is the title of Shelley[’\']s chapter \d+, concerning (.+)\?$',
                  r'Which heading best matches the topic of \1?', text)
    text = re.sub(r'^According to Wiersbe[’\']s biographical chronology, which', 'Which', text)
    text = re.sub(r'^Which person has a biography titled (.+) in Wiersbe[’\']s collection of Christian leaders\?$',
                  r'Which Christian leader is associated with the biographical heading \1?', text)
    text = re.sub(r'\bthe NIV section\b', 'the section', text)
    text = re.sub(r'\bNIV Bible reference\b', 'Bible reference', text)
    return text


def source_reference(payload):
    for key in ('source', 'source_reference', 'bible_reference', 'reference', 'source_book'):
        value = payload.get(key)
        if value:
            return (value if isinstance(value, str) else json.dumps(value, ensure_ascii=False))[:1500]
    return 'A source reference is not available for this question.'


def source_citation(payload):
    detail = payload.get('source_detail') or {}
    title = payload.get('source_book') or detail.get('book') or ''
    reference = source_reference(payload)
    parts = [str(title)] if title else []
    if detail.get('type') == 'character_study':
        parts.append('Bible Character Studies: ' + detail['character'])
    if detail.get('type') == 'quiz_book':
        parts.append(detail['source_location'])
    if reference and reference not in parts:
        parts.append(reference)
    page = payload.get('source_page') or detail.get('pdf_page_start')
    if page:
        parts.append('Page ' + str(page))
    return ' · '.join(parts)


def reference_question(payload):
    """Recognize reference answers without depending on one question's wording."""
    pattern = r'(?:[1-3]\s*)?[A-Za-z]+(?:\s+[A-Za-z]+){0,2}\s+\d+:\d+(?:\s*[-–]\s*\d+)?'
    return (payload.get('category') == 'Identify the Bible Verse'
            or all(re.fullmatch(pattern, str(option).strip()) for option in payload['options']))


def scripture_context(payload):
    """Read complete local verse text with adjacent context, without reference labels."""
    database = current_app.config.get('BIBLE_STUDY_DATABASE')
    if not database:
        return None
    reference = source_reference(payload)
    match = re.fullmatch(r'(.+?)\s+(\d+):(\d+)(?:\s*[-–]\s*(\d+))?', reference.strip())
    if not match:
        return None
    from bible_study import get_indexed_niv_passage
    book, chapter, first = match[1], int(match[2]), int(match[3])
    last = int(match[4]) if match[4] else first
    # At chapter boundaries the complete target verse still takes priority.
    ranges = dict.fromkeys(((max(1, first - 1), last + 1), (max(1, first - 1), last), (first, last)))
    for start, end in ranges:
        passage = get_indexed_niv_passage(f'{book} {chapter}:{start}-{end}', database,
            static_folder=current_app.static_folder,
            protected_paths=(current_app.config.get('DATABASE'), current_app.config.get('AI_DATABASE'),
                             current_app.config['HEAVENS_MILLIONAIRE_DATABASE']))
        if passage:
            return passage
    return None


def source_hint(payload):
    """Reference-identification hints give passage text, keeping the answer hidden."""
    if reference_question(payload):
        passage = scripture_context(payload)
        if not passage:
            raise GameError('The full passage is temporarily unavailable. Your Bible Hint has not been used. Please choose another lifeline or answer the question.', 409)
        return 'Read the passage in context:\n\n' + passage
    reference = source_reference(payload)
    kind = payload.get('source_type') or (payload.get('source_detail') or {}).get('type')
    if kind == 'scripture':
        return reference
    chapter = re.search(r'\bchapter\s+(\d+)\b', reference, re.I)
    page = payload.get('source_page') or (payload.get('source_detail') or {}).get('pdf_page_start')
    if chapter or page:
        return 'Historical source location: ' + ', '.join(
            part for part in ('chapter ' + chapter[1] if chapter else '', 'page ' + str(page) if page else '') if part) + '.'
    if re.search(r'\bG\d{1,5}\b', reference):
        return 'Language clue: consider the Greek word and its meaning.'
    if re.search(r'\bH\d{1,5}\b', reference):
        return 'Language clue: consider the Hebrew word and its meaning.'
    return 'Consider the context: ' + str(payload.get('category', 'Bible knowledge')) + '.'


def pastor_advice(question):
    """One saved simulated call. Confidence is a clue, never a correctness flag."""
    options = json.loads(question['choices_json'])
    removed = set(json.loads(question['removed_json']))
    right = RNG.random() < .75
    suggested = question['correct_choice'] if right else RNG.choice(
        [i for i in range(4) if i != question['correct_choice'] and i not in removed])
    # Confident advice may be wrong; hesitant advice may still be right.
    confident = RNG.random() < (.8 if right else .4)
    letter = 'ABCD'[suggested]
    pick = f'{letter}: {options[suggested]}'
    if confident:
        reply = RNG.choice([
            f'I remember this one. I would choose {pick}. I am quite confident about that.',
            f'Yes, this sounds familiar. My answer is {pick}. That is the choice I would go with.',
        ])
        farewell = 'You are welcome! All the best with the rest of the game.'
    else:
        reply = RNG.choice([
            f'Hmm... I am not completely sure. I think it could be {pick}, but I might be mistaken.',
            f'Let me think... I am leaning toward {pick}. Please treat that as my best guess.',
        ])
        farewell = 'You are welcome. I wish I could be more certain. Take a moment and trust your judgment.'
    payload = json.loads(question['question_json'])
    return dict(name='Pastor Pijeme', portrait_url='/static/heavens_millionaire/pastor_pijeme.webp',
        simulated=True, confidence='confident' if confident else 'unsure',
        suggested_choice=suggested, suggested_answer=options[suggested],
        dialogue=[
            dict(speaker='Host', text='Hello, Pastor Pijeme! Could you help our player with a Bible question?'),
            dict(speaker='Pastor Pijeme', text='Hello! Of course. Let me hear the question.'),
            dict(speaker='Host', text=question_prompt(payload)),
            dict(speaker='Pastor Pijeme', text=reply),
            dict(speaker='Host', text='Thank you, Pastor! The final choice is with our player.'),
            dict(speaker='Pastor Pijeme', text=farewell),
        ])


def question_pool(payload):
    return payload.get('question_pool', 'previous')


def repeat_keys(payload):
    wording = re.sub(r'[^a-z0-9]+', ' ', unicodedata.normalize('NFKD', question_prompt(payload)).lower()).strip()
    return {'id:' + payload['id'], 'wording:' + hashlib.sha256(wording.encode()).hexdigest()}


def remember_question(conn, player_id, payload, seen_at):
    keys = repeat_keys(payload)
    keys.update(row[0] for row in conn.execute(
        'SELECT repeat_key FROM game_question_repeat_keys WHERE question_id=?', (payload['id'],)))
    conn.executemany('''INSERT OR IGNORE INTO game_player_question_history(player_id,repeat_key,first_seen_at)
        VALUES(?,?,?)''', [(player_id, key, seen_at) for key in keys])


def available_questions(stage, excluded, avoid_categories=(), player_id=None, pool=None):
    """Draw within the level, excluding adjacent categories and reused study chapters."""
    excluded = set(excluded)
    avoid_categories = set(avoid_categories)
    used_families = set()
    if excluded:
        placeholders = ','.join('?' for _ in excluded)
        for row in db().execute(f'SELECT payload FROM game_questions WHERE id IN ({placeholders})', tuple(excluded)):
            family = json.loads(row['payload']).get('question_family')
            if family:
                used_families.add(family)
    sql = 'SELECT q.id,q.payload FROM game_questions q WHERE q.stage=? AND q.enabled=1'
    parameters = [stage]
    if player_id:
        sql += ''' AND NOT EXISTS(SELECT 1 FROM game_question_repeat_keys k
            JOIN game_player_question_history h ON h.repeat_key=k.repeat_key
            WHERE k.question_id=q.id AND h.player_id=?)'''
        parameters.append(player_id)
    if excluded:
        sql += f''' AND NOT EXISTS(SELECT 1 FROM game_question_repeat_keys k
            JOIN game_question_repeat_keys used ON used.repeat_key=k.repeat_key
            WHERE k.question_id=q.id AND used.question_id IN ({placeholders}))'''
        parameters.extend(excluded)
    rows = db().execute(sql, parameters).fetchall()
    eligible = []
    for row in rows:
        if row['id'] in excluded:
            continue
        payload = json.loads(row['payload'])
        if (payload['category'] not in avoid_categories and payload.get('question_family') not in used_families
                and (pool is None or question_pool(payload) == pool)):
            eligible.append((row['id'], payload))
    return eligible


def choose(stage, excluded, avoid_categories=(), player_id=None, pool=None):
    eligible = available_questions(stage, excluded, avoid_categories, player_id, pool)
    if not eligible:
        raise GameError('No unseen replacement is available for this level and source. Your current question is unchanged.', 409)
    question_id, payload = RNG.choice(eligible)
    indices = list(range(4))
    RNG.shuffle(indices)
    return question_id, payload, [payload['options'][i] for i in indices], indices.index(payload['correct_index'])


def plan_game(player_id):
    """Find 15 unseen slots with six quiz-book questions and nine previous questions."""
    candidates = {stage: available_questions(stage, [], player_id=player_id) for stage in set(STAGES)}
    keys = {}
    for row in db().execute('SELECT question_id,repeat_key FROM game_question_repeat_keys'):
        keys.setdefault(row[0], set()).add(row[1])
    attempts = 0

    def arrange(position, remaining_new, used_keys, used_families, previous_category):
        nonlocal attempts
        if position == len(STAGES):
            return [] if remaining_new == 0 else None
        if attempts >= 5000:
            return None
        # Prune unavailable source/level combinations before trying a sequence.
        minimum_new = maximum_new = 0
        for stage, count in Counter(STAGES[position:]).items():
            available = Counter(question_pool(q) for ident, q in candidates[stage]
                if not keys[ident] & used_keys and q.get('question_family') not in used_families)
            if sum(available.values()) < count:
                return None
            minimum_new += max(0, count - available['previous'])
            maximum_new += min(count, available['quiz_books'])
        if not minimum_new <= remaining_new <= maximum_new:
            return None
        remaining = len(STAGES) - position
        options = [(ident,q) for ident,q in candidates[STAGES[position]]
            if q['category'] != previous_category and not keys[ident] & used_keys
            and q.get('question_family') not in used_families
            and (question_pool(q) != 'quiz_books' or remaining_new > 0)
            and (question_pool(q) != 'previous' or remaining_new < remaining)]
        RNG.shuffle(options)
        for ident, payload in options:
            attempts += 1
            family = payload.get('question_family')
            following = arrange(position + 1, remaining_new - (question_pool(payload) == 'quiz_books'),
                used_keys | keys[ident], used_families | ({family} if family else set()), payload['category'])
            if following is not None:
                return [(ident,payload)] + following
            if attempts >= 5000:
                break
        return None

    selected = arrange(0, 6, set(), set(), None)
    if selected is None:
        if attempts >= 5000:
            raise GameError('A fresh game could not be arranged. Please try again.', 409)
        raise GameError('There are not enough unseen questions to prepare a fresh game at every level. More questions must be added; your question history will not be reset.', 409)
    result = []
    for ident, payload in selected:
        indices = list(range(4)); RNG.shuffle(indices)
        result.append((ident,payload,[payload['options'][i] for i in indices],indices.index(payload['correct_index'])))
    return result


@game.before_request
def protect_game():
    request.max_content_length = 6 * 1024 * 1024
    request.max_form_parts = 24
    if request.content_length and request.content_length > 6 * 1024 * 1024:
        raise GameError('The upload is too large. Please use a smaller photo.', 413)
    if request.method == 'POST':
        supplied = request.headers.get('X-CSRF-Token') or body().get('csrf', '')
        if not isinstance(supplied, str) or not hmac.compare_digest(supplied, session.get('hm_csrf', '')) or not supplied:
            raise GameError('Your page session expired. Reload and try again.', 403)


@game.errorhandler(GameError)
def game_error(exc):
    return jsonify(error=exc.message), exc.status


@game.errorhandler(413)
def oversized_upload(exc):
    return jsonify(error='The upload is too large. Please use a photo smaller than 5 MB.'), 413


@game.after_request
def private_response(response):
    response.headers['Cache-Control'] = 'private, no-store'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Referrer-Policy'] = 'same-origin'
    return response


@game.get('/api/bootstrap')
def bootstrap():
    owner = player(False)
    identity = site_account()
    with transaction():
        sweep()
    active = db().execute("SELECT * FROM game_sessions WHERE player_id=? AND status='active'", (owner['id'],)).fetchone() if owner else None
    return jsonify(csrf=csrf(), player=public_player(owner), site_user=dict(name=identity['name']) if identity else None,
                   admin=bool(administrator()), directory=directory(), points=POINTS, stages=STAGES,
                   active=state(active) if active else None)


@game.post('/api/register')
def register_player():
    rate_limit('registration', 8, 3600)
    name, password = str(body().get('name', '')).strip(), str(body().get('password', ''))
    if not 2 <= len(name) <= 100 or len(password) < 8 or len(password) > 128:
        raise GameError('Enter your complete name and a password of 8–128 characters.')
    church = next((c for c in directory() if c['id'] == body().get('church_id')), None)
    if church is None or church['area'] != body().get('area'):
        raise GameError('Please select an Area and its church from the directory.')
    if body().get('consent') not in ('true', 'on', True):
        raise GameError('Please confirm the portrait and scoreboard notice.')
    filename = portrait(request.files.get('photo'))
    ident, code, recovery = secrets.token_hex(16), 'HM-' + secrets.token_hex(5).upper(), secrets.token_urlsafe(24)
    try:
        with transaction() as conn:
            conn.execute('''INSERT INTO game_players(id,player_code,name,area,church_id,church,pastor,photo,consented_at,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)''', (ident, code, name, church['area'], church['id'], church['church'], church['pastor'], filename, now(), now()))
            conn.execute('INSERT INTO game_player_credentials(player_id,password_hash,recovery_hash) VALUES(?,?,?)',
                         (ident, generate_password_hash(password), generate_password_hash(recovery)))
            audit('register', ident)
    except Exception:
        delete_portrait(filename)
        raise
    owner = db().execute('SELECT * FROM game_players WHERE id=?', (ident,)).fetchone()
    sign_in(owner)
    return jsonify(player=public_player(owner), recovery_code=recovery, csrf=csrf())


@game.post('/api/site-login')
def site_login():
    identity = site_account()
    if not identity:
        raise GameError('Please sign in through the District website first.', 401)
    church = account_church(identity)
    with transaction() as conn:
        owner = conn.execute('SELECT * FROM game_players WHERE site_key=?', (identity['key'],)).fetchone()
        if owner is None:
            ident = secrets.token_hex(16)
            conn.execute('''INSERT INTO game_players(id,player_code,site_key,name,area,church_id,church,pastor,created_at)
                VALUES(?,?,?,?,?,?,?,?,?)''', (ident, 'HM-' + secrets.token_hex(5).upper(), identity['key'], identity['name'],
                church['area'], church['id'], church['church'], church['pastor'], now()))
            conn.execute('INSERT INTO game_player_credentials(player_id) VALUES(?)', (ident,))
            audit('link_website_account', ident)
            owner = conn.execute('SELECT * FROM game_players WHERE id=?', (ident,)).fetchone()
        else:
            conn.execute('UPDATE game_players SET name=?,area=?,church_id=?,church=?,pastor=? WHERE id=?',
                         (identity['name'], church['area'], church['id'], church['church'], church['pastor'], owner['id']))
            owner = conn.execute('SELECT * FROM game_players WHERE id=?', (owner['id'],)).fetchone()
    sign_in(owner)
    return jsonify(player=public_player(owner), csrf=csrf())


@game.post('/api/login')
def login():
    code = str(body().get('player_code', '')).strip().upper()
    password = str(body().get('password', ''))[:129]
    rate_limit('login', 30, identity=code)
    rate_limit('login-ip', 100)
    row = db().execute('''SELECT p.*,c.password_hash FROM game_players p JOIN game_player_credentials c ON c.player_id=p.id
        WHERE p.player_code=? AND p.site_key IS NULL''', (code,)).fetchone()
    # Dummy hash work avoids a quick response for nonexistent player IDs.
    encoded = row['password_hash'] if row else current_app.extensions['heavens_millionaire']['dummy_hash']
    if not check_password_hash(encoded or current_app.extensions['heavens_millionaire']['dummy_hash'], password):
        raise GameError('The Player ID or password is incorrect.', 401)
    if not row:
        raise GameError('The Player ID or password is incorrect.', 401)
    sign_in(row)
    return jsonify(player=public_player(row), csrf=csrf())


@game.post('/api/logout')
def logout():
    for key in ('hm_player', 'hm_auth_version', 'hm_csrf'):
        session.pop(key, None)
    return jsonify(ok=True, csrf=csrf())


@game.post('/api/recover')
def recover():
    code = str(body().get('player_code', '')).strip().upper()
    rate_limit('recovery', 5, identity=code)
    rate_limit('recovery-ip', 20)
    password = str(body().get('password', ''))
    supplied = str(body().get('recovery_code', ''))[:200]
    if not 8 <= len(password) <= 128:
        raise GameError('Use a new password of 8–128 characters.')
    replacement = secrets.token_urlsafe(24)
    with transaction() as conn:
        row = conn.execute('''SELECT p.id,c.recovery_hash FROM game_players p JOIN game_player_credentials c
            ON c.player_id=p.id WHERE player_code=? AND site_key IS NULL''', (code,)).fetchone()
        valid = bool(row and row['recovery_hash'] and check_password_hash(row['recovery_hash'], supplied))
        temporary = None
        if row and not valid:
            requests = conn.execute("SELECT * FROM game_recovery_requests WHERE player_id=? AND status='approved' AND expires_at>?", (row['id'], now())).fetchall()
            temporary = next((r for r in requests if check_password_hash(r['token_hash'], supplied)), None)
            valid = temporary is not None
        if not valid:
            raise GameError('The Player ID or recovery code is incorrect or expired.', 401)
        conn.execute('UPDATE game_player_credentials SET password_hash=?,recovery_hash=?,auth_version=auth_version+1 WHERE player_id=?',
                     (generate_password_hash(password), generate_password_hash(replacement), row['id']))
        conn.execute("UPDATE game_recovery_requests SET status='used',token_hash=NULL WHERE player_id=? AND status IN ('approved','pending')", (row['id'],))
        audit('password_recovered', row['id'])
    for key in ('hm_player', 'hm_auth_version'):
        session.pop(key, None)
    return jsonify(ok=True, recovery_code=replacement, player_code=code)


@game.post('/api/recovery-help')
def recovery_help():
    rate_limit('recovery-help', 5, 3600)
    code = str(body().get('player_code', '')).strip().upper()
    with transaction() as conn:
        row = conn.execute('SELECT id FROM game_players WHERE player_code=? AND site_key IS NULL', (code,)).fetchone()
        if row and not conn.execute("SELECT 1 FROM game_recovery_requests WHERE player_id=? AND status='pending'", (row['id'],)).fetchone():
            conn.execute("INSERT INTO game_recovery_requests(player_id,created_at,status,note) VALUES(?,?,'pending',?)",
                         (row['id'], now(), str(body().get('note', ''))[:300]))
    return jsonify(ok=True, message='If that Player ID exists, a recovery request is available to the game administrator. Ask your pastor or administrator to verify you in person.')


@game.post('/api/photo')
def update_photo():
    owner = player()
    rate_limit('photo', 15, 3600, owner['id'])
    if owner['site_key']:
        church = account_church(site_account())
    else:
        church = next((c for c in directory() if c['id'] == body().get('church_id')), None)
        if not church or church['area'] != body().get('area'):
            raise GameError('Please confirm your Area and church.')
    if body().get('consent') not in ('true', 'on', True):
        raise GameError('Please confirm the portrait and scoreboard notice.')
    filename = portrait(request.files.get('photo'))
    try:
        with transaction() as conn:
            old_photo = conn.execute('SELECT photo FROM game_players WHERE id=?', (owner['id'],)).fetchone()[0]
            conn.execute('UPDATE game_players SET photo=?,consented_at=?,area=?,church_id=?,church=?,pastor=? WHERE id=?',
                         (filename, now(), church['area'], church['id'], church['church'], church['pastor'], owner['id']))
            audit('portrait_updated', owner['id'])
    except Exception:
        delete_portrait(filename)
        raise
    delete_portrait(old_photo)
    return jsonify(player=public_player(db().execute('SELECT * FROM game_players WHERE id=?', (owner['id'],)).fetchone()))


@game.get('/photo/<ident>')
def photo(ident):
    if not player(False) and not administrator():
        raise GameError('Sign in to view player portraits.', 401)
    row = db().execute('SELECT photo FROM game_players WHERE id=?', (ident,)).fetchone()
    if not row or not re.fullmatch(r'[a-f0-9]{48}\.jpg', row['photo'] or ''):
        raise GameError('Portrait unavailable.', 404)
    path = Path(current_app.config['HEAVENS_MILLIONAIRE_PHOTOS']) / row['photo']
    if not path.is_file():
        raise GameError('Portrait unavailable.', 404)
    return send_file(path, mimetype='image/jpeg')


@game.post('/api/start')
def start():
    owner = player()
    if not owner['photo'] or not owner['consented_at'] or not owner['church_id']:
        raise GameError('Please finish your player profile and add your photo before playing.', 403)
    rate_limit('start', 60, 3600, owner['id'])
    start_key = str(body().get('request_id', ''))
    if not re.fullmatch(r'[a-zA-Z0-9_-]{16,80}', start_key):
        raise GameError('A valid start request ID is required.')
    with transaction() as conn:
        sweep()
        old = conn.execute('SELECT * FROM game_sessions WHERE player_id=? AND start_key=?', (owner['id'], start_key)).fetchone()
        active = conn.execute("SELECT * FROM game_sessions WHERE player_id=? AND status='active'", (owner['id'],)).fetchone()
        if old or active:
            return jsonify(state=state(old or active))
        ident = secrets.token_hex(24)
        conn.execute('''INSERT INTO game_sessions(id,player_id,start_key,created_at,area,church,pastor,policy_json)
            VALUES(?,?,?,?,?,?,?,?)''', (ident, owner['id'], start_key, now(), owner['area'], owner['church'], owner['pastor'], json.dumps(policy())))
        seen = []
        for position, (qid, payload, options, correct) in enumerate(plan_game(owner['id']), 1):
            seen.append(qid)
            conn.execute('''INSERT INTO game_answers(session_id,position,question_id,token,question_json,choices_json,correct_choice,deadline,started_at)
                VALUES(?,?,?,?,?,?,?,?,?)''', (ident, position, qid, secrets.token_hex(20), json.dumps(payload), json.dumps(options), correct,
                                               now() + SECONDS[0] if position == 1 else None, now() if position == 1 else None))
            if position == 1:
                remember_question(conn, owner['id'], payload, now())
        conn.execute('UPDATE game_sessions SET seen_json=? WHERE id=?', (json.dumps(seen), ident))
        audit('game_started', owner['id'], dict(session_id=ident))
        return jsonify(state=state(conn.execute('SELECT * FROM game_sessions WHERE id=?', (ident,)).fetchone()))


@game.get('/api/state/<ident>')
def get_state(ident):
    owner = player()
    with transaction():
        attempt = attempt_for(owner, ident)
        return jsonify(state=state(attempt))


@game.post('/api/action')
def action():
    owner = player()
    rate_limit('game-action', 180, 60, owner['id'])
    data = body()
    with transaction() as conn:
        attempt = attempt_for(owner, str(data.get('session_id', '')))
        if attempt['status'] != 'active':
            return jsonify(state=state(attempt))
        question = round_row(attempt)
        if data.get('token') != question['token']:
            # Retries of a previously resolved question cannot affect the next one.
            previous = conn.execute('SELECT outcome FROM game_answers WHERE session_id=? AND token=?', (attempt['id'], str(data.get('token', '')))).fetchone()
            if previous and previous['outcome'] is not None:
                return jsonify(state=state(attempt))
            raise GameError('This question has changed. Please refresh the game state.', 409)
        kind = data.get('action')
        if kind in ('fifty', 'audience', 'hint', 'change') and question['pause_until'] and now() < question['pause_until']:
            raise GameError('Please let the current lifeline animation finish.', 409)
        if kind == 'resume':
            latest = conn.execute('''SELECT kind FROM game_lifeline_usage
                WHERE session_id=? AND position=? ORDER BY id DESC LIMIT 1''',
                (attempt['id'], attempt['position'])).fetchone()
            # A retry from an earlier dialog cannot resume a newer lifeline.
            if data.get('lifeline') and (not latest or data['lifeline'] != latest['kind']):
                return jsonify(state=state(attempt))
            resume_question(question)
            return jsonify(state=state(attempt))
        if kind in ('answer', 'walk', 'offer', 'accept_offer', 'reject_offer', 'fifty', 'audience', 'hint', 'change'):
            # Acting on the question also resumes older clients that have not
            # explicitly closed their lifeline. Invalid actions roll this back.
            resume_question(question)
            question = round_row(attempt)
        if kind == 'next':
            if question['outcome'] != 'correct':
                raise GameError('Resolve this question first.', 409)
            position = attempt['position'] + 1
            conn.execute('UPDATE game_sessions SET position=? WHERE id=?', (position, attempt['id']))
            conn.execute('UPDATE game_answers SET started_at=?,deadline=? WHERE session_id=? AND position=?',
                         (now(), now() + SECONDS[position - 1], attempt['id'], position))
            remember_question(conn, owner['id'], json.loads(round_row(dict(attempt, position=position))['question_json']), now())
        elif question['outcome'] is not None:
            return jsonify(state=state(attempt))
        elif kind == 'answer':
            selected = data.get('choice')
            if type(selected) is not int or selected not in range(4):
                raise GameError('Please select an available answer.')
            if 'choice_text' in data:
                # Grade the visible choice against this round's frozen options. A
                # re-render/lifeline must never turn a selected answer into another.
                options = json.loads(question['choices_json'])
                text = data['choice_text']
                if not isinstance(text, str) or text not in options:
                    raise GameError('The answer choices have changed. Please select your answer again.', 409)
                selected = options.index(text)
            if selected in json.loads(question['removed_json']):
                raise GameError('Please select an available answer.')
            correct = selected == question['correct_choice']
            conn.execute('UPDATE game_answers SET answer_choice=?,outcome=?,resolved_at=? WHERE id=?',
                         (selected, 'correct' if correct else 'wrong', now(), question['id']))
            if correct:
                earned = POINTS[attempt['position'] - 1]
                guaranteed = earned if attempt['position'] in (5, 10, 15) else attempt['guaranteed']
                conn.execute('UPDATE game_sessions SET earned=?,guaranteed=? WHERE id=?', (earned, guaranteed, attempt['id']))
                if attempt['position'] == 15:
                    finish(attempt, 'won', earned)
            else:
                finish(attempt, 'wrong', attempt['guaranteed'])
        elif kind == 'walk':
            finish(attempt, 'walkaway', attempt['earned'])
        elif kind in ('offer', 'accept_offer', 'reject_offer'):
            rules = json.loads(attempt['policy_json'])
            if not rules['offers_enabled'] or attempt['position'] not in rules['offer_questions'] or attempt['earned'] < rules['minimum_points']:
                raise GameError('No sweetened offer is available for this question.')
            offer = conn.execute('SELECT * FROM game_offers WHERE session_id=? AND position=?', (attempt['id'], attempt['position'])).fetchone()
            if kind == 'offer' and offer is None:
                total = max(attempt['earned'], min(rules['maximum_offer'], attempt['earned'] + rules['bonus_points']))
                conn.execute("INSERT INTO game_offers(session_id,position,base_points,bonus,total,status,created_at) VALUES(?,?,?,?,?,'pending',?)",
                             (attempt['id'], attempt['position'], attempt['earned'], total - attempt['earned'], total, now()))
            elif kind != 'offer':
                if not offer or offer['status'] != 'pending':
                    raise GameError('This offer has already been resolved.', 409)
                conn.execute('UPDATE game_offers SET status=? WHERE id=?', ('accepted' if kind == 'accept_offer' else 'rejected', offer['id']))
                if kind == 'accept_offer':
                    finish(attempt, 'sweetened', offer['total'])
        elif kind in ('fifty', 'audience', 'hint', 'change'):
            if conn.execute('SELECT 1 FROM game_lifeline_usage WHERE session_id=? AND kind=?', (attempt['id'], kind)).fetchone():
                raise GameError('That lifeline has already been used.', 409)
            pause_started = now()
            result = {}
            if kind == 'fifty':
                result['removed'] = RNG.sample([i for i in range(4) if i != question['correct_choice']], 2)
                conn.execute('UPDATE game_answers SET removed_json=? WHERE id=?', (json.dumps(result['removed']), question['id']))
            elif kind == 'hint':
                call = pastor_advice(question)
                # Retain the original slot/key for saved games and older clients.
                result.update(pastor=call, hint=call['dialogue'][3]['text'])
            elif kind == 'audience':
                available = [i for i in range(4) if i not in json.loads(question['removed_json'])]
                accuracy = {'Beginner': .90, 'Intermediate': .75, 'Advanced': .60, 'Expert': .48, 'Master': .35}[STAGES[attempt['position'] - 1]]
                favored = question['correct_choice'] if RNG.random() < accuracy else RNG.choice([i for i in available if i != question['correct_choice']])
                votes = [0] * 4
                for _ in range(100):
                    votes[favored if RNG.random() < .55 else RNG.choice(available)] += 1
                result.update(percentages=votes, simulated=True)
            else:
                seen = json.loads(attempt['seen_json'])
                # Use saved snapshots: an edited bank must not change a neighbor's category.
                nearby = conn.execute('''SELECT question_json FROM game_answers
                    WHERE session_id=? AND position BETWEEN ? AND ?''',
                    (attempt['id'], attempt['position'] - 1, attempt['position'] + 1)).fetchall()
                avoid_categories = {json.loads(row['question_json'])['category'] for row in nearby}
                qid, payload, options, correct = choose(STAGES[attempt['position'] - 1], seen, avoid_categories,
                    owner['id'], question_pool(json.loads(question['question_json'])))
                seen.append(qid)
                result = dict(replaced=question['question_id'], replacement=qid)
                conn.execute('UPDATE game_answers SET question_id=?,token=?,question_json=?,choices_json=?,correct_choice=?,removed_json=\'[]\' WHERE id=?',
                             (qid, secrets.token_hex(20), json.dumps(payload), json.dumps(options), correct, question['id']))
                conn.execute('UPDATE game_sessions SET seen_json=? WHERE id=?', (json.dumps(seen), attempt['id']))
                remember_question(conn, owner['id'], payload, now())
            # Freeze the exact remaining time until the lifeline is closed.
            conn.execute('UPDATE game_answers SET pause_started_at=?,pause_until=? WHERE id=?',
                         (pause_started, now() + 2, question['id']))
            conn.execute('INSERT INTO game_lifeline_usage(session_id,position,kind,used_at,result_json) VALUES(?,?,?,?,?)',
                         (attempt['id'], attempt['position'], kind, now(), json.dumps(result)))
        else:
            raise GameError('Unknown game action.')
        return jsonify(state=state(conn.execute('SELECT * FROM game_sessions WHERE id=?', (attempt['id'],)).fetchone()))


@game.post('/api/interruption')
def interruption():
    owner = player()
    data = body()
    event = str(data.get('event_id', ''))
    if not re.fullmatch(r'[a-zA-Z0-9_-]{16,80}', event):
        raise GameError('Invalid interruption event.')
    with transaction() as conn:
        attempt = attempt_for(owner, str(data.get('session_id', '')))
        question = round_row(attempt)
        if attempt['status'] != 'active' or question['outcome'] is not None:
            return jsonify(state=state(attempt))
        previous = conn.execute('SELECT MAX(created_at) FROM game_interruption_logs WHERE session_id=?', (attempt['id'],)).fetchone()[0]
        if not conn.execute('SELECT 1 FROM game_interruption_logs WHERE session_id=? AND event_id=?', (attempt['id'], event)).fetchone() and (previous is None or now() - previous >= 2):
            conn.execute('INSERT INTO game_interruption_logs(session_id,event_id,created_at,kind) VALUES(?,?,?,?)',
                         (attempt['id'], event, now(), str(data.get('kind', 'visibility'))[:40]))
            conn.execute('UPDATE game_sessions SET interruptions=interruptions+1 WHERE id=?', (attempt['id'],))
            if attempt['interruptions'] >= 1:
                finish(attempt, 'disqualified', 0)
        return jsonify(state=state(conn.execute('SELECT * FROM game_sessions WHERE id=?', (attempt['id'],)).fetchone()))


def result_filters():
    conditions, params = ["s.status!='active'"], []
    for name in ('area', 'church'):
        if request.args.get(name):
            conditions.append('s.' + name + '=?')
            params.append(request.args[name])
    period = request.args.get('period', 'all')
    if period in ('today', 'week', 'month'):
        stamp = datetime.fromtimestamp(now(), ZoneInfo('Asia/Manila')).replace(hour=0, minute=0, second=0, microsecond=0)
        if period == 'week':
            stamp -= timedelta(days=stamp.weekday())
        elif period == 'month':
            stamp = stamp.replace(day=1)
        conditions.append('s.finished_at>=?')
        params.append(stamp.timestamp())
    return conditions, params


def result_records(conditions, params, top=False, page=1, limit=20):
    where = ' AND '.join(conditions)
    duration = 'CASE WHEN s.finished_at>=s.created_at THEN CAST(ROUND((s.finished_at-s.created_at)*1000) AS INTEGER) END'
    columns = f's.*,p.name,p.player_code,p.photo,{duration} AS elapsed_ms'
    score_order = 'final_points DESC,(elapsed_ms IS NULL) ASC,elapsed_ms ASC'
    query = f'SELECT {columns} FROM game_sessions s JOIN game_players p ON p.id=s.player_id WHERE {where}'
    if top:
        query = f'''SELECT * FROM (SELECT ranked.*,RANK() OVER(ORDER BY {score_order}) AS rank FROM
            (SELECT source.*,ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY {score_order},finished_at ASC,id ASC) AS best FROM ({query}) source) ranked
            WHERE best=1)'''
    total = db().execute('SELECT count(*) FROM (' + query + ')', params).fetchone()[0]
    order = score_order + ',finished_at ASC,id ASC' if top or request.args.get('sort') == 'score' else 'finished_at DESC,id DESC'
    records = db().execute(query + ' ORDER BY ' + order + ' LIMIT ? OFFSET ?', params + [limit, (page - 1) * limit]).fetchall()
    result = []
    for row in records:
        result.append(dict(id=row['id'], player_id=row['player_id'], name=row['name'], area=row['area'], church=row['church'],
                           points=row['final_points'], position=row['position'], date=row['finished_at'], result=row['status'], elapsed_ms=row['elapsed_ms'],
                           eligible=row['status'] != 'disqualified', rank=row['rank'] if top else None,
                           photo_url=f"/bible-games/photo/{row['player_id']}?v={row['photo']}" if row['photo'] else ''))
    return dict(records=result, total=total, page=page, pages=max(1, math.ceil(total / limit)))


@game.get('/api/leaderboard')
def leaderboard():
    player()
    with transaction():
        sweep()
    conditions, params = result_filters()
    top = request.args.get('view', 'top') == 'top'
    if top:
        conditions.append("s.status!='disqualified'")
    try:
        page = max(1, min(100000, int(request.args.get('page', 1))))
    except ValueError:
        raise GameError('Invalid page.')
    return jsonify(result_records(conditions, params, top, page))


def profile_data(owner_id):
    owner = db().execute('SELECT * FROM game_players WHERE id=?', (owner_id,)).fetchone()
    if not owner:
        raise GameError('Player not found.', 404)
    stats = db().execute('''SELECT count(*) AS games,max(CASE WHEN status!='disqualified' THEN final_points ELSE 0 END) AS highest,
        sum(CASE WHEN status!='disqualified' THEN final_points ELSE 0 END) AS cumulative,max(position) AS highest_question,
        sum(CASE WHEN status='won' THEN 1 ELSE 0 END) AS victories FROM game_sessions WHERE player_id=? AND status!='active' ''', (owner_id,)).fetchone()
    latest = db().execute("SELECT final_points FROM game_sessions WHERE player_id=? AND status!='active' ORDER BY finished_at DESC LIMIT 1", (owner_id,)).fetchone()
    current = next((c for c in directory() if c['id'] == owner['church_id']), None)
    highest_stage = STAGES[stats['highest_question'] - 1] if stats['highest_question'] else 'Not played'
    return dict(player=public_player(owner), stats=dict(stats, recent=latest[0] if latest else 0, highest_stage=highest_stage), current_church=current)


@game.get('/api/profile')
def profile():
    owner = player()
    with transaction():
        sweep()
    try:
        page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        raise GameError('Invalid page.')
    data = profile_data(owner['id'])
    data.update(history=result_records(["s.status!='active'", 's.player_id=?'], [owner['id']], page=page))
    return jsonify(data)


@game.get('/api/admin')
def admin_data():
    if not administrator():
        raise GameError('District administrator access is required.', 403)
    with transaction():
        sweep()
    search = '%' + str(request.args.get('search', ''))[:80] + '%'
    players = [public_player(r) for r in db().execute('SELECT * FROM game_players WHERE name LIKE ? OR player_code LIKE ? ORDER BY created_at DESC LIMIT 50', (search, search))]
    stats = dict(db().execute('''SELECT count(*) AS attempts,sum(status='active') AS active,
        sum(status='won') AS wins,sum(status='disqualified') AS disqualified FROM game_sessions''').fetchone())
    stats['players'] = db().execute('SELECT count(*) FROM game_players').fetchone()[0]
    by_date = [dict(r) for r in db().execute("SELECT date(created_at,'unixepoch','+8 hours') AS date,count(*) AS games FROM game_sessions GROUP BY date ORDER BY date DESC LIMIT 30")]
    by_church = [dict(r) for r in db().execute("SELECT area,church,count(*) AS games,max(final_points) AS highest FROM game_sessions WHERE status NOT IN ('active','disqualified') GROUP BY area,church ORDER BY highest DESC LIMIT 100")]
    pending = [dict(r) for r in db().execute("""SELECT r.id,r.created_at,r.status,r.note,p.name,p.player_code,p.area,p.church FROM game_recovery_requests r
        JOIN game_players p ON p.id=r.player_id WHERE r.status IN ('pending','approved') ORDER BY r.id DESC LIMIT 50""")]
    return jsonify(stats=stats, policy=policy(), players=players, by_date=by_date, by_church=by_church, recovery_requests=pending,
                   audit=[dict(r) for r in db().execute('SELECT created_at,actor,action,details FROM game_audit_logs ORDER BY id DESC LIMIT 50')],
                   question_counts=[dict(r) for r in db().execute('SELECT stage,count(*) AS count FROM game_questions WHERE enabled=1 GROUP BY stage')])


@game.get('/api/admin/player/<ident>')
def admin_player(ident):
    if not administrator():
        raise GameError('District administrator access is required.', 403)
    data = profile_data(ident)
    data['history'] = result_records(["s.status!='active'", 's.player_id=?'], [ident], limit=100)
    data['interruptions'] = [dict(r) for r in db().execute('''SELECT i.* FROM game_interruption_logs i JOIN game_sessions s
        ON s.id=i.session_id WHERE s.player_id=? ORDER BY i.id DESC LIMIT 100''', (ident,))]
    return jsonify(data)


@game.get('/api/admin/questions')
def admin_questions():
    if not administrator():
        raise GameError('District administrator access is required.', 403)
    try:
        page = max(1, int(request.args.get('page', 1)))
    except ValueError:
        raise GameError('Invalid page.')
    questions = []
    for row in db().execute('SELECT payload FROM game_questions WHERE enabled=1 ORDER BY stage,id LIMIT 20 OFFSET ?', ((page - 1) * 20,)):
        payload = json.loads(row[0])
        payload.setdefault('difficulty_rating', list(dict.fromkeys(STAGES)).index(payload['stage']) + 1)
        payload.setdefault('difficulty_reason', 'Existing question: retained at its established difficulty level.')
        questions.append(payload)
    return jsonify(questions=questions, page=page, total=db().execute('SELECT count(*) FROM game_questions WHERE enabled=1').fetchone()[0])


@game.post('/api/admin/policy')
def admin_policy():
    admin = administrator()
    if not admin:
        raise GameError('District administrator access is required.', 403)
    data = body()
    positions = data.get('offer_questions')
    if not isinstance(positions, list) or any(type(p) is not int or p not in range(2, 16) for p in positions) or len(set(positions)) != len(positions):
        raise GameError('Offer questions must be unique numbers from 2 to 15.')
    rules = dict(offers_enabled=data.get('offers_enabled') is True, offer_questions=positions)
    for key in ('bonus_points', 'minimum_points', 'maximum_offer'):
        value = data.get(key)
        if type(value) is not int or not 0 <= value <= 1000000:
            raise GameError('Offer amounts must be whole numbers from 0 to 1,000,000.')
        rules[key] = value
    with transaction() as conn:
        conn.execute("INSERT OR REPLACE INTO game_settings VALUES('policy',?)", (json.dumps(rules),))
        audit('policy_updated', admin['key'], rules)
    return jsonify(policy=rules)


@game.post('/api/admin/recovery')
def admin_recovery():
    admin = administrator()
    if not admin:
        raise GameError('District administrator access is required.', 403)
    note = str(body().get('verification_note', '')).strip()
    if len(note) < 10:
        raise GameError('Record how the person was independently verified (at least 10 characters).')
    code = secrets.token_urlsafe(24)
    with transaction() as conn:
        row = conn.execute("SELECT * FROM game_recovery_requests WHERE id=? AND status='pending'", (body().get('request_id'),)).fetchone()
        if not row:
            raise GameError('This pending request was not found.', 404)
        conn.execute("UPDATE game_recovery_requests SET status='approved',token_hash=?,expires_at=? WHERE id=?",
                     (generate_password_hash(code), now() + 900, row['id']))
        audit('recovery_verified', admin['key'], dict(request_id=row['id'], verification_note=note[:500]))
    return jsonify(reset_code=code, expires_in=900)


@game.get('/api/admin/export')
def export():
    admin = administrator()
    if not admin:
        raise GameError('District administrator access is required.', 403)
    conditions, params = result_filters()
    rows = db().execute('''SELECT p.player_code,p.name,s.area,s.church,s.pastor,s.final_points,s.position,s.status,s.created_at,s.finished_at
        FROM game_sessions s JOIN game_players p ON p.id=s.player_id WHERE ''' + ' AND '.join(conditions) + ' ORDER BY s.created_at DESC', params).fetchall()
    output = io.StringIO(newline='')
    writer = csv.writer(output)
    writer.writerow(['Player ID', 'Name', 'Area', 'Church', 'Pastor', 'Points', 'Question reached', 'Result', 'Started UTC', 'Finished UTC'])
    for row in rows:
        values = list(row)
        for i in (8, 9):
            values[i] = datetime.fromtimestamp(values[i], ZoneInfo('UTC')).isoformat() if values[i] else ''
        # Escape spreadsheet formulas in user-supplied names and church labels.
        writer.writerow(["'" + v if isinstance(v, str) and v.lstrip().startswith(('=', '+', '-', '@', '\t', '\r')) else v for v in values])
    with transaction():
        audit('results_exported', admin['key'], dict(rows=len(rows)))
    return Response('\ufeff' + output.getvalue(), mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=heavens-millionaire-results.csv'})


def initialize(path, question_path):
    """Idempotent schema/import. Editorial flags do not determine eligibility."""
    question_file = Path(question_path)
    raw = question_file.read_bytes()
    bank = json.loads(raw.decode('utf-8-sig'))
    questions = bank['questions']
    seen = set()
    for q in questions:
        if (not isinstance(q.get('id'), str) or q['id'] in seen or q.get('stage') not in set(STAGES)
                or not isinstance(q.get('question'), str) or not q['question'].strip()
                or ('question_family' in q and (not isinstance(q['question_family'], str) or not q['question_family'].strip()))
                or question_pool(q) not in ('previous', 'quiz_books')
                or ('difficulty_rating' in q and (type(q['difficulty_rating']) is not int
                    or q['difficulty_rating'] != list(dict.fromkeys(STAGES)).index(q['stage']) + 1))
                or any(not isinstance(q.get(field), str) or not q[field].strip() for field in ('category', 'explanation', 'source', 'source_book'))
                or not isinstance(q.get('options'), list) or len(q['options']) != 4
                or any(not isinstance(o, str) or not o.strip() for o in q['options'])
                or len(set(o.strip().casefold() for o in q['options'])) != 4
                or type(q.get('correct_index')) is not int or q['correct_index'] not in range(4)
                or q.get('answer') != q['options'][q['correct_index']]):
            raise ValueError('Invalid or duplicate game question: ' + str(q.get('id')))
        seen.add(q['id'])
    expected = dict(Beginner=400, Intermediate=400, Advanced=400, Expert=300, Master=100)
    if ({s: sum(q['stage'] == s and question_pool(q) == 'previous' for q in questions) for s in expected} != expected
            or bank.get('question_count') != len(questions)):
        raise ValueError('Expected the complete original 1,600-question pool plus the declared additional questions.')
    bank_keys = bank.get('repeat_keys_by_id', {})
    if not isinstance(bank_keys, dict):
        raise ValueError('Invalid question repeat keys.')
    for q in questions:
        extra = bank_keys.get(q['id'], [])
        if not isinstance(extra, list) or any(not isinstance(key, str) or not key or len(key) > 180 for key in extra):
            raise ValueError('Invalid question repeat keys: ' + q['id'])
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path, timeout=10)) as conn, conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA busy_timeout=10000')
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS game_settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS game_players(id TEXT PRIMARY KEY,player_code TEXT UNIQUE NOT NULL,site_key TEXT UNIQUE,
            name TEXT NOT NULL,area TEXT NOT NULL DEFAULT '',church_id TEXT NOT NULL DEFAULT '',church TEXT NOT NULL DEFAULT '',
            pastor TEXT NOT NULL DEFAULT '',photo TEXT,consented_at REAL,created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS game_player_credentials(player_id TEXT PRIMARY KEY REFERENCES game_players(id),
            password_hash TEXT,recovery_hash TEXT,auth_version INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS game_questions(id TEXT PRIMARY KEY,stage TEXT NOT NULL,category TEXT NOT NULL,payload TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1);
        CREATE INDEX IF NOT EXISTS game_question_stage ON game_questions(stage,enabled);
        CREATE TABLE IF NOT EXISTS game_question_repeat_keys(question_id TEXT NOT NULL,repeat_key TEXT NOT NULL,
            PRIMARY KEY(question_id,repeat_key));
        CREATE INDEX IF NOT EXISTS game_repeat_key_lookup ON game_question_repeat_keys(repeat_key,question_id);
        CREATE TABLE IF NOT EXISTS game_player_question_history(player_id TEXT NOT NULL REFERENCES game_players(id),
            repeat_key TEXT NOT NULL,first_seen_at REAL NOT NULL,PRIMARY KEY(player_id,repeat_key));
        CREATE TABLE IF NOT EXISTS game_sessions(id TEXT PRIMARY KEY,player_id TEXT NOT NULL REFERENCES game_players(id),start_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',position INTEGER NOT NULL DEFAULT 1,earned INTEGER NOT NULL DEFAULT 0,
            guaranteed INTEGER NOT NULL DEFAULT 0,final_points INTEGER,interruptions INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,finished_at REAL,area TEXT NOT NULL,church TEXT NOT NULL,pastor TEXT NOT NULL,
            policy_json TEXT NOT NULL,seen_json TEXT NOT NULL DEFAULT '[]',UNIQUE(player_id,start_key));
        CREATE UNIQUE INDEX IF NOT EXISTS game_one_active ON game_sessions(player_id) WHERE status='active';
        CREATE INDEX IF NOT EXISTS game_results_score ON game_sessions(status,final_points DESC,finished_at);
        CREATE INDEX IF NOT EXISTS game_results_scope ON game_sessions(area,church,finished_at);
        CREATE INDEX IF NOT EXISTS game_player_history ON game_sessions(player_id,finished_at DESC);
        CREATE TABLE IF NOT EXISTS game_answers(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES game_sessions(id),position INTEGER NOT NULL,
            question_id TEXT NOT NULL,token TEXT UNIQUE NOT NULL,question_json TEXT NOT NULL,choices_json TEXT NOT NULL,correct_choice INTEGER NOT NULL,
            answer_choice INTEGER,outcome TEXT,started_at REAL,deadline REAL,pause_until REAL,pause_started_at REAL,resolved_at REAL,removed_json TEXT NOT NULL DEFAULT '[]',UNIQUE(session_id,position));
        CREATE INDEX IF NOT EXISTS game_answer_deadline ON game_answers(deadline,outcome);
        CREATE TABLE IF NOT EXISTS game_lifeline_usage(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES game_sessions(id),position INTEGER NOT NULL,
            kind TEXT NOT NULL,used_at REAL NOT NULL,result_json TEXT NOT NULL,UNIQUE(session_id,kind));
        CREATE TABLE IF NOT EXISTS game_interruption_logs(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES game_sessions(id),event_id TEXT NOT NULL,
            created_at REAL NOT NULL,kind TEXT NOT NULL,UNIQUE(session_id,event_id));
        CREATE TABLE IF NOT EXISTS game_offers(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES game_sessions(id),position INTEGER NOT NULL,
            base_points INTEGER NOT NULL,bonus INTEGER NOT NULL,total INTEGER NOT NULL,status TEXT NOT NULL,created_at REAL NOT NULL,UNIQUE(session_id,position));
        CREATE TABLE IF NOT EXISTS game_audit_logs(id INTEGER PRIMARY KEY,created_at REAL NOT NULL,actor TEXT NOT NULL,action TEXT NOT NULL,details TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS game_rate_limits(bucket TEXT PRIMARY KEY,started_at REAL NOT NULL,count INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS game_recovery_requests(id INTEGER PRIMARY KEY,player_id TEXT NOT NULL REFERENCES game_players(id),created_at REAL NOT NULL,
            status TEXT NOT NULL,note TEXT NOT NULL DEFAULT '',token_hash TEXT,expires_at REAL);
        ''')
        if 'pause_started_at' not in {row[1] for row in conn.execute('PRAGMA table_info(game_answers)')}:
            conn.execute('ALTER TABLE game_answers ADD COLUMN pause_started_at REAL')
        digest = hashlib.sha256(raw).hexdigest()
        old = conn.execute("SELECT value FROM game_settings WHERE key='bank_hash'").fetchone()
        changed = not old or old[0] != digest
        if changed:
            conn.execute('UPDATE game_questions SET enabled=0')
            conn.executemany('''INSERT INTO game_questions(id,stage,category,payload,enabled) VALUES(?,?,?,?,1)
                ON CONFLICT(id) DO UPDATE SET stage=excluded.stage,category=excluded.category,payload=excluded.payload,enabled=1''',
                [(q['id'], q['stage'], q.get('category', ''), json.dumps(q, ensure_ascii=False)) for q in questions])
            conn.execute("INSERT OR REPLACE INTO game_settings VALUES('bank_hash',?)", (digest,))
        history_ready = conn.execute("SELECT 1 FROM game_settings WHERE key='question_history_v1'").fetchone()
        if changed or not history_ready:
            for q in questions:
                conn.execute('DELETE FROM game_question_repeat_keys WHERE question_id=?', (q['id'],))
            conn.executemany('INSERT OR IGNORE INTO game_question_repeat_keys VALUES(?,?)',
                [(q['id'],key) for q in questions for key in repeat_keys(q) | set(bank_keys.get(q['id'],[]))])
            # Backfill only displayed rounds, plus discarded lifeline originals. Unreached
            # preselected slots are still new to the player and remain eligible later.
            for row in conn.execute('''SELECT s.player_id,a.question_json,a.started_at,s.created_at
                    FROM game_answers a JOIN game_sessions s ON s.id=a.session_id
                    WHERE a.started_at IS NOT NULL OR a.outcome IS NOT NULL''').fetchall():
                remember_question(conn, row[0], json.loads(row[1]), row[2] or row[3])
            for row in conn.execute('''SELECT s.player_id,u.result_json,u.used_at FROM game_lifeline_usage u
                    JOIN game_sessions s ON s.id=u.session_id WHERE u.kind='change' ''').fetchall():
                replaced = json.loads(row[1]).get('replaced')
                original = conn.execute('SELECT payload FROM game_questions WHERE id=?', (replaced,)).fetchone()
                if original:
                    remember_question(conn,row[0],json.loads(original[0]),row[2])
                elif replaced:
                    conn.execute('INSERT OR IGNORE INTO game_player_question_history VALUES(?,?,?)',
                        (row[0],'id:' + replaced,row[2]))
            conn.execute("INSERT OR REPLACE INTO game_settings VALUES('question_history_v1','1')")
        conn.execute("INSERT OR REPLACE INTO game_settings VALUES('schema_version','2')")


def register_heavens_millionaire(app, get_local_db):
    app.config.setdefault('HEAVENS_MILLIONAIRE_DATABASE', data_path('heavens_millionaire.db'))
    app.config.setdefault('HEAVENS_MILLIONAIRE_PHOTOS', data_path('heavens_millionaire_photos'))
    app.config.setdefault('HEAVENS_MILLIONAIRE_QUESTIONS', str(Path(__file__).with_name('heavens_millionaire_questions.json')))
    initialize(app.config['HEAVENS_MILLIONAIRE_DATABASE'], app.config['HEAVENS_MILLIONAIRE_QUESTIONS'])
    app.extensions['heavens_millionaire'] = dict(local_db=get_local_db, dummy_hash=generate_password_hash(secrets.token_urlsafe(24)))
    app.register_blueprint(game)

    @app.teardown_appcontext
    def close_game_connection(error):
        connection = g.pop('millionaire_db', None)
        if connection is not None:
            connection.close()


def game_page():
    response = make_response(render_template('bible_games.html'))
    response.headers['Cache-Control'] = 'private, no-store'
    return response
