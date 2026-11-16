"""Isolated engine, account/portrait security and main-app integration checks."""
import io
import hashlib
import json
import os
import re
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from unittest.mock import patch

from flask import Flask
from PIL import Image

import heavens_millionaire as hm

ROOT = Path(__file__).resolve().parents[1]


class MillionaireTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.local = sqlite3.connect(':memory:', check_same_thread=False)
        self.local.row_factory = sqlite3.Row
        self.local.executescript('''CREATE TABLE sheet_accounts_cache(username TEXT,name TEXT,age TEXT,sex TEXT,church_address TEXT,position TEXT);
            INSERT INTO sheet_accounts_cache VALUES('pastor','Test Pastor','1','Test Church','Test Address','Pastor');
            INSERT INTO sheet_accounts_cache VALUES('another','Another Pastor','2','Another Church','Other Address','Pastor');
            INSERT INTO sheet_accounts_cache VALUES('admin','Test Admin','1','Test Church','Test Address','District Overseer');
            INSERT INTO sheet_accounts_cache VALUES('overseer','Test Overseer','3',NULL,NULL,'Area Overseer');
            CREATE TABLE sheet_members_account_cache(username TEXT,name TEXT,area_number TEXT,church_id TEXT,church_address TEXT,pastor TEXT);
            INSERT INTO sheet_members_account_cache VALUES('member','Test Member','1','Test Church','Test Address','Test Pastor');
            CREATE TRIGGER forbid_directory_update BEFORE UPDATE ON sheet_accounts_cache BEGIN SELECT RAISE(FAIL,'Directory write'); END;
            CREATE TRIGGER forbid_directory_insert BEFORE INSERT ON sheet_accounts_cache BEGIN SELECT RAISE(FAIL,'Directory write'); END;''')
        self.addCleanup(self.local.close)
        self.clock = 1800000000.
        self.app = Flask(__name__, root_path=str(ROOT))
        self.app.config.update(TESTING=True, SECRET_KEY='offline-game-test', HEAVENS_MILLIONAIRE_DATABASE=str(self.directory/'game.db'),
                               HEAVENS_MILLIONAIRE_PHOTOS=str(self.directory/'photos'), HEAVENS_MILLIONAIRE_CLOCK=lambda:self.clock)
        hm.register_heavens_millionaire(self.app, lambda:self.local)
        self.app.add_url_rule('/bible-games', 'bible_games', hm.game_page)
        self.client = self.app.test_client()
        self.bootstrap()

    def bootstrap(self):
        data = self.client.get('/bible-games/api/bootstrap').get_json()
        self.csrf = data['csrf']
        self.church = data['directory'][0]
        return data

    def photo(self):
        output = io.BytesIO()
        image = Image.new('RGB', (900,700), '#325782')
        image.save(output, 'JPEG', exif=Image.Exif())
        output.seek(0)
        return output

    def post(self, route, data):
        response = self.client.post('/bible-games/api/'+route, json=data, headers={'X-CSRF-Token':self.csrf})
        result=response.get_json()
        if result and 'csrf' in result:
            self.csrf=result['csrf']
        return response

    def register(self):
        data=dict(name='Sample Player',area=self.church['area'],church_id=self.church['id'],password='a-good-test-password',consent='true',photo=(self.photo(),'portrait.jpg'))
        response=self.client.post('/bible-games/api/register',data=data,headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,200,response.get_json())
        self.account=response.get_json();self.csrf=self.account['csrf']
        return self.account

    def start(self):
        response=self.post('start',dict(request_id='a-unique-start-request'))
        self.assertEqual(response.status_code,200,response.get_json())
        self.state=response.get_json()['state']
        return self.state

    def action(self, action, **extra):
        response=self.post('action',dict(session_id=self.state['id'],token=self.state['token'],action=action,**extra))
        if response.status_code==200:self.state=response.get_json()['state']
        return response

    def correct_index(self):
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            return conn.execute('SELECT correct_choice FROM game_answers WHERE token=?',(self.state['token'],)).fetchone()[0]

    def advance(self, count):
        for _ in range(count):
            self.assertEqual(self.action('answer',choice=self.correct_index()).status_code,200)
            if self.state['status']=='active':self.action('next')

    def test_import_all_questions_regardless_of_review_and_idempotent_startup(self):
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM game_questions WHERE enabled=1').fetchone()[0],3382)
            self.assertEqual(conn.execute('PRAGMA journal_mode').fetchone()[0],'wal')
        self.register();self.start()
        hm.initialize(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'],ROOT/'heavens_millionaire_questions.json')
        self.assertEqual(self.client.get('/bible-games/api/state/'+self.state['id']).get_json()['state']['id'],self.state['id'])
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            before=conn.execute('SELECT question_id,question_json,choices_json,correct_choice FROM game_answers ORDER BY position').fetchall()
        revised=json.loads((ROOT/'heavens_millionaire_questions.json').read_text(encoding='utf-8'))
        updated=next(q for q in revised['questions'] if q['id']==before[0][0])
        updated['question']='An editorial update for future games only?'
        path=self.directory/'revised-bank.json';path.write_text(json.dumps(revised),encoding='utf-8')
        hm.initialize(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'],path)
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            self.assertEqual(before,conn.execute('SELECT question_id,question_json,choices_json,correct_choice FROM game_answers ORDER BY position').fetchall())
            self.assertEqual(json.loads(conn.execute('SELECT payload FROM game_questions WHERE id=?',(before[0][0],)).fetchone()[0])['question'],updated['question'])
        self.assertEqual(self.client.get('/bible-games/api/state/'+self.state['id']).get_json()['state']['question'],self.state['question'])

    def test_registration_photo_compression_metadata_privacy_and_directory(self):
        result=self.register()
        self.assertEqual(result['player']['pastor'],'Test Pastor')
        files=list((self.directory/'photos').iterdir());self.assertEqual(len(files),1)
        self.assertLessEqual(files[0].stat().st_size,60*1024)
        with Image.open(files[0]) as photo:
            self.assertEqual(photo.size,(320,320));self.assertFalse(photo.getexif())
        response=self.client.get(result['player']['photo_url']);self.assertEqual(response.status_code,200);response.close()
        stranger=self.app.test_client()
        self.assertEqual(stranger.get(result['player']['photo_url']).status_code,401)
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            hashes=conn.execute('SELECT password_hash,recovery_hash FROM game_player_credentials').fetchone()
            self.assertNotIn('a-good-test-password',hashes);self.assertNotIn(result['recovery_code'],hashes)

    def test_photo_and_consent_required_and_invalid_images_rejected(self):
        for photo in (None,(io.BytesIO(b'not an image'),'pretend.jpg')):
            data=dict(name='Sample Player',area=self.church['area'],church_id=self.church['id'],password='a-good-test-password',consent='true')
            if photo:data['photo']=photo
            response=self.client.post('/bible-games/api/register',data=data,headers={'X-CSRF-Token':self.csrf})
            self.assertEqual(response.status_code,400)
        response=self.client.post('/bible-games/api/register',data=dict(name='Player',area=self.church['area'],church_id=self.church['id'],password='a-good-test-password',photo=(self.photo(),'photo.jpg')),headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,400)

    def test_csrf_and_other_players_cannot_modify_attempt(self):
        self.register();self.start()
        self.assertEqual(self.client.post('/bible-games/api/action',json=dict(action='answer')).status_code,403)
        self.assertEqual(self.client.post('/bible-games/api/action',json=['not-an-object'],headers={'X-CSRF-Token':self.csrf}).status_code,400)
        self.assertEqual(self.client.post('/bible-games/api/action',data='{',content_type='application/json',headers={'X-CSRF-Token':self.csrf}).status_code,400)
        stranger=self.app.test_client();token=stranger.get('/bible-games/api/bootstrap').get_json()['csrf']
        self.assertEqual(stranger.post('/bible-games/api/action',json=dict(session_id=self.state['id'],action='walk'),headers={'X-CSRF-Token':token}).status_code,401)

    def test_fifteen_stages_private_answers_frozen_questions_and_victory(self):
        self.register();self.start()
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            self.assertEqual(conn.execute('SELECT count(DISTINCT question_id) FROM game_answers').fetchone()[0],15)
        for position in range(1,16):
            self.assertEqual(self.state['position'],position);self.assertEqual(self.state['stage'],hm.STAGES[position-1])
            self.assertEqual(self.state['total_seconds'],hm.SECONDS[position-1])
            self.assertNotIn('correct_choice',self.state);self.assertNotIn('explanation',self.state)
            token=self.state['token'];index=self.correct_index()
            self.action('answer',choice=index)
            self.assertEqual(self.state['earned'],hm.POINTS[position-1])
            self.assertEqual(self.state['correct_choice'],index)
            # Repeat an answer: it cannot award again or advance twice.
            self.post('action',dict(session_id=self.state['id'],token=token,action='answer',choice=index))
            if position<15:self.action('next')
        self.assertEqual(self.state['status'],'won');self.assertEqual(self.state['final_points'],1000000)
        profile=self.client.get('/bible-games/api/profile').get_json()
        self.assertEqual(profile['stats']['victories'],1);self.assertEqual(profile['stats']['cumulative'],1000000)
        self.assertEqual(profile['stats']['highest_stage'],'Master')

    def test_wrong_answer_zero_and_milestones(self):
        for milestone,expected in ((0,0),(5,1000),(10,32000)):
            with self.subTest(milestone=milestone):
                if milestone==0:self.register()
                self.state=self.post('start',dict(request_id=f'unique-milestone-{milestone}')).get_json()['state']
                self.advance(milestone)
                self.action('answer',choice=(self.correct_index()+1)%4)
                self.assertEqual(self.state['status'],'wrong');self.assertEqual(self.state['final_points'],expected)

    def test_refresh_network_timeouts_and_one_active_attempt(self):
        self.register();self.start();deadline=self.state['deadline']
        again=self.post('start',dict(request_id='another-unique-start-key')).get_json()['state']
        self.assertEqual(again['id'],self.state['id'])
        self.clock+=10
        refreshed=self.client.get('/bible-games/api/state/'+self.state['id']).get_json()['state']
        self.assertEqual(refreshed['deadline'],deadline)
        self.clock=deadline+1
        self.action('answer',choice=self.correct_index())
        self.assertEqual(self.state['status'],'timeout');self.assertEqual(self.state['final_points'],0)
        self.assertEqual(self.client.get('/bible-games/api/leaderboard?view=all').get_json()['total'],1)

    def test_lifelines_once_only_pauses_and_question_replacement(self):
        self.register();self.start();correct=self.correct_index();deadline=self.state['deadline'];old_token=self.state['token']
        self.action('fifty');self.assertEqual(len(self.state['removed']),2);self.assertNotIn(correct,self.state['removed'])
        self.assertEqual(self.state['deadline'],deadline+2)
        self.clock+=3
        self.assertEqual(self.action('fifty').status_code,409)
        self.action('audience');self.assertEqual(sum(self.state['lifeline_results']['audience']['percentages']),100)
        self.clock+=3;self.action('hint');self.assertTrue(self.state['lifeline_results']['hint']['hint'])
        self.clock+=3;self.action('change');self.assertNotEqual(old_token,self.state['token'])
        self.assertEqual(self.state['deadline'],deadline+8)
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            seen=json.loads(conn.execute('SELECT seen_json FROM game_sessions').fetchone()[0]);self.assertEqual(len(seen),16)
        self.assertEqual(self.post('action',dict(session_id=self.state['id'],token=old_token,action='answer',choice=correct)).status_code,409)

    def test_standard_and_sweetened_walkaway_recorded(self):
        self.register();self.start();self.advance(1);self.action('walk');self.assertEqual(self.state['final_points'],100)
        self.state=self.post('start',dict(request_id='another-game-for-offer')).get_json()['state']
        self.advance(5);self.action('offer');total=self.state['offer']['total'];self.action('accept_offer')
        self.assertEqual(self.state['status'],'sweetened');self.assertEqual(self.state['final_points'],total)

    def test_interruption_debounce_first_warning_second_disqualification(self):
        self.register();self.start()
        def interrupt(ident):
            self.state=self.post('interruption',dict(session_id=self.state['id'],event_id=ident,kind='visibility')).get_json()['state']
        interrupt('first-unique-interruption');self.assertEqual(self.state['interruptions'],1);self.assertEqual(self.state['status'],'active')
        interrupt('first-unique-interruption');interrupt('another-transient-event');self.assertEqual(self.state['interruptions'],1)
        self.clock+=3;interrupt('second-unique-interruption');self.assertEqual(self.state['status'],'disqualified')
        self.assertEqual(self.client.get('/bible-games/api/leaderboard').get_json()['total'],0)
        self.assertEqual(self.client.get('/bible-games/api/leaderboard?view=all').get_json()['total'],1)

    def test_password_recovery_rotates_code_preserves_progress_invalidates_sessions(self):
        account=self.register();self.start();self.action('walk')
        result=self.post('recover',dict(player_code=account['player']['player_code'],recovery_code=account['recovery_code'],password='the-replacement-password'))
        self.assertEqual(result.status_code,200)
        self.assertEqual(self.client.get('/bible-games/api/profile').status_code,401)
        self.assertEqual(self.post('recover',dict(player_code=account['player']['player_code'],recovery_code=account['recovery_code'],password='another-password')).status_code,401)
        self.assertEqual(self.post('login',dict(player_code=account['player']['player_code'],password='the-replacement-password')).status_code,200)
        self.assertEqual(self.client.get('/bible-games/api/profile').get_json()['stats']['games'],1)

    def test_website_link_photo_gate_admin_authorization_and_snapshot_policy(self):
        with self.client.session_transaction() as signed:
            signed['pastor_logged_in']=True;signed['pastor_username']='pastor'
        result=self.post('site-login',{}).get_json();self.assertEqual(result['player']['name'],'Test Pastor')
        self.assertFalse(result['player']['photo']);self.assertEqual(self.post('start',dict(request_id='unique-photo-gate-request')).status_code,403)
        uploaded=self.client.post('/bible-games/api/photo',data=dict(consent='true',photo=(self.photo(),'portrait.jpg')),headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(uploaded.status_code,200);self.assertTrue(uploaded.get_json()['player']['photo'])
        self.assertEqual((uploaded.get_json()['player']['area'],uploaded.get_json()['player']['church'],uploaded.get_json()['player']['pastor']),('1','Test Church','Test Pastor'))
        self.assertEqual(self.post('start',dict(request_id='unique-after-photo-request')).status_code,200)
        self.assertEqual(self.client.get('/bible-games/api/admin').status_code,403)
        with self.client.session_transaction() as signed:
            signed.clear();signed['ao_logged_in']=True;signed['ao_username']='admin'
        self.bootstrap()
        self.assertEqual(self.client.get('/bible-games/api/admin').status_code,200)
        self.assertEqual(self.client.get('/bible-games/api/admin/questions').get_json()['total'],3382)
        self.assertEqual(self.post('admin/policy',dict(offers_enabled=True,offer_questions=[6],bonus_points=200,minimum_points=1000,maximum_offer=1000000)).status_code,200)

    def test_linked_photo_ignores_submitted_affiliation_and_repairs_old_selection(self):
        with self.client.session_transaction() as signed:
            signed['pastor_logged_in']=True;signed['pastor_username']='pastor'
        owner=self.post('site-login',{}).get_json()['player']
        other=next(c for c in self.bootstrap()['directory'] if c['area']=='2')
        response=self.client.post('/bible-games/api/photo',data=dict(area='2',church_id=other['id'],consent='true',photo=(self.photo(),'portrait.jpg')),headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,200)
        saved=response.get_json()['player']
        self.assertEqual((saved['area'],saved['church'],saved['pastor']),('1','Test Church','Test Pastor'))
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn,conn:
            conn.execute("UPDATE game_players SET area='2',church='Wrong old church' WHERE id=?",(owner['id'],))
        linked=self.post('site-login',{}).get_json()['player']
        self.assertEqual(linked['id'],owner['id']);self.assertTrue(linked['photo'])
        self.assertEqual((linked['area'],linked['church']),('1','Test Church'))

    def test_member_photo_uses_account_when_directory_has_no_pastor_row(self):
        self.local.execute("UPDATE sheet_members_account_cache SET church_id='Member Church',pastor='Recorded Pastor' WHERE username='member'")
        with self.client.session_transaction() as signed:
            signed['member_logged_in']=True;signed['username']='member'
        result=self.post('site-login',{})
        self.assertEqual(result.status_code,200)
        response=self.client.post('/bible-games/api/photo',data=dict(consent='true',photo=(self.photo(),'portrait.jpg')),headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,200)
        saved=response.get_json()['player']
        self.assertEqual((saved['area'],saved['church'],saved['pastor']),('1','Member Church','Recorded Pastor'))
        self.assertEqual(self.post('start',dict(request_id='linked-member-start-request')).status_code,200)

    def test_game_only_photo_still_requires_its_church_selection(self):
        self.register()
        response=self.client.post('/bible-games/api/photo',data=dict(consent='true',photo=(self.photo(),'portrait.jpg')),headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,400)

    def test_area_overseer_needs_no_church_and_photo_only_preserves_role(self):
        with self.client.session_transaction() as signed:
            signed['ao_logged_in']=True;signed['ao_username']='overseer'
        result=self.post('site-login',{})
        self.assertEqual(result.status_code,200)
        owner=result.get_json()['player']
        self.assertEqual((owner['area'],owner['church'],owner['pastor']),('3','Area Overseer',''))
        self.assertEqual(self.post('start',dict(request_id='overseer-before-photo')).status_code,403)
        response=self.client.post('/bible-games/api/photo',data=dict(consent='true',photo=(self.photo(),'portrait.jpg')),headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.get_json()['player']['church'],'Area Overseer')
        self.assertEqual(self.post('start',dict(request_id='overseer-after-photo')).status_code,200)
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            self.assertEqual(conn.execute('SELECT area,church,pastor FROM game_sessions').fetchone(),('3','Area Overseer',''))

    def test_area_overseer_choice_does_not_grant_game_administration(self):
        choice=next(c for c in self.bootstrap()['directory'] if c['area']=='1' and c['church']=='Area Overseer')
        response=self.client.post('/bible-games/api/register',data=dict(name='Game Overseer',area='1',church_id=choice['id'],password='a-good-test-password',consent='true',photo=(self.photo(),'portrait.jpg')),headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,200)
        self.csrf=response.get_json()['csrf']
        self.assertEqual(response.get_json()['player']['church'],'Area Overseer')
        self.assertEqual(self.client.get('/bible-games/api/admin').status_code,403)

    def test_bad_import_fails_without_changing_bank(self):
        invalid=json.loads((ROOT/'heavens_millionaire_questions.json').read_text(encoding='utf-8'));invalid['questions'][0]['correct_index']=8
        path=self.directory/'invalid.json';path.write_text(json.dumps(invalid),encoding='utf-8')
        with self.assertRaises(ValueError):hm.initialize(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'],path)
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:self.assertEqual(conn.execute('SELECT count(*) FROM game_questions').fetchone()[0],3382)

    def test_duplicate_concurrent_answers_and_forged_scores(self):
        from concurrent.futures import ThreadPoolExecutor
        self.register();self.start();correct=self.correct_index()
        cookie=self.client.get_cookie('session').value
        def submit():
            client=self.app.test_client();client.set_cookie('session',cookie)
            return client.post('/bible-games/api/action',json=dict(session_id=self.state['id'],token=self.state['token'],action='answer',choice=correct,points=1000000),headers={'X-CSRF-Token':self.csrf})
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses=list(pool.map(lambda _:submit(),range(2)))
        self.assertEqual([r.status_code for r in responses],[200,200])
        self.state=responses[0].get_json()['state'];self.assertEqual(self.state['earned'],100)
        self.action('next');self.action('walk',points=1000000)
        self.assertEqual(self.state['final_points'],100)
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM game_answers WHERE outcome='correct'").fetchone()[0],1)

    def test_rankings_ties_filters_zero_results_and_csv(self):
        account=self.register();self.start();self.action('walk')
        board=self.client.get('/bible-games/api/leaderboard?view=top&area=1&period=today').get_json()
        self.assertEqual(board['total'],1);self.assertEqual(board['records'][0]['points'],0)
        self.assertEqual(board['records'][0]['rank'],1)
        self.assertEqual(self.client.get('/bible-games/api/leaderboard?area=2').get_json()['total'],0)
        self.assertEqual(self.client.get('/bible-games/api/admin/export').status_code,403)
        with self.client.session_transaction() as signed:
            signed['ao_logged_in']=True;signed['ao_username']='admin'
        exported=self.client.get('/bible-games/api/admin/export')
        self.assertEqual(exported.status_code,200);self.assertIn(account['player']['player_code'],exported.get_data(as_text=True))

    def test_settings_are_frozen_and_rejected_offer_is_recorded(self):
        self.register();self.start()
        with self.client.session_transaction() as signed:
            signed['ao_logged_in']=True;signed['ao_username']='admin'
        self.assertEqual(self.post('admin/policy',dict(offers_enabled=True,offer_questions=[6],bonus_points=9000,minimum_points=1000,maximum_offer=1000000)).status_code,200)
        self.advance(5);self.action('offer');self.assertEqual(self.state['offer']['total'],2000)
        self.action('reject_offer');self.assertEqual(self.state['offer']['status'],'rejected');self.assertEqual(self.state['status'],'active')
        self.assertEqual(self.action('accept_offer').status_code,409)

    def test_admin_assisted_recovery_requires_verification_and_token_is_single_use(self):
        account=self.register()
        self.post('recovery-help',dict(player_code=account['player']['player_code'],note='I need help.'))
        self.assertEqual(self.post('admin/recovery',dict(request_id=1,verification_note='verified in person')).status_code,403)
        with self.client.session_transaction() as signed:
            signed['ao_logged_in']=True;signed['ao_username']='admin'
        self.assertEqual(self.post('admin/recovery',dict(request_id=1,verification_note='')).status_code,400)
        result=self.post('admin/recovery',dict(request_id=1,verification_note='Verified in person with the registered church pastor.'))
        self.assertEqual(result.status_code,200);code=result.get_json()['reset_code']
        payload=dict(player_code=account['player']['player_code'],recovery_code=code,password='recovered-test-password')
        self.assertEqual(self.post('recover',payload).status_code,200)
        self.assertEqual(self.post('recover',payload).status_code,401)

    def test_church_mismatch_boolean_answers_and_overlapping_pauses_rejected(self):
        self.register();self.start()
        self.assertEqual(self.action('answer',choice=True).status_code,400)
        self.action('fifty');deadline=self.state['deadline']
        self.assertEqual(self.action('hint').status_code,409)
        current=self.client.get('/bible-games/api/state/'+self.state['id']).get_json()['state']
        self.assertEqual(current['deadline'],deadline)
        self.clock+=3
        removed=current['removed'][0]
        self.assertEqual(self.action('answer',choice=removed).status_code,400)

    def test_source_book_titles_hidden_until_explanation(self):
        questions=json.loads((ROOT/'heavens_millionaire_questions.json').read_text(encoding='utf-8'))['questions']
        forbidden=r'Thompson|Wiersbe|Shelley|Church History in Plain|50 People Every|\bNIV\b'
        for question in questions:
            self.assertIsNone(re.search(forbidden,hm.question_prompt(question),re.I),question['id'])
            self.assertIsNone(re.search(forbidden,hm.source_hint(question),re.I),question['id'])
            self.assertIn(question['source_book'],hm.source_citation(question))
        study=next(q for q in questions if q.get('source_detail',{}).get('type')=='character_study')
        self.assertNotIn('Thompson',hm.question_prompt(study))
        self.assertIn('Bible Character Studies: '+study['source_detail']['character'],hm.source_citation(study))
        self.register();self.start();self.assertNotIn('source',self.state)
        self.action('answer',choice=self.correct_index());self.assertTrue(self.state['source'])

    def test_heading_questions_replaced_and_other_questions_preserved(self):
        from collections import Counter
        questions=json.loads((ROOT/'heavens_millionaire_questions.json').read_text(encoding='utf-8'))['questions']
        studies=[q for q in questions if q.get('source_detail',{}).get('type')=='character_study']
        retained=[q for q in questions if q not in studies and hm.question_pool(q)=='previous']
        digest=lambda data:hashlib.sha256(json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        self.assertEqual(len(studies),1035)
        self.assertEqual(len({q['question'] for q in studies}),1035)
        self.assertEqual(Counter(q['stage'] for q in studies),dict(Beginner=340,Intermediate=270,Advanced=290,Expert=135))
        self.assertEqual({q['source_detail']['character'] for q in studies},
                         {'Noah','Abraham','Jacob','Joseph','Joshua','Gideon','Samuel','David','Solomon','Elijah','Elisha','Daniel'})
        self.assertEqual(len(retained),565)
        self.assertEqual(digest(retained),'5b5660c184a6729c0c4481b588bee1c84cac7e9fcb29f4a3a438f238839dae35')
        self.assertEqual(digest([q for q in questions if q['stage']=='Master' and hm.question_pool(q)=='previous']),
                         '61e5a6b52d9b1a1edba83cdb5b912b2940cf3b62b64afb5cd4344fcf948ee5f0')
        for q in questions:
            if q.get('source_type')=='scripture':
                self.assertIsNone(re.search(r'heading|headed|section|chapter titled|biographical heading',q['question'],re.I),q['id'])

    def test_each_level_draws_from_all_categories_except_its_neighbors(self):
        with self.app.app_context():
            conn=hm.db()
            for stage in set(hm.STAGES):
                expected={r['id'] for r in conn.execute('SELECT id FROM game_questions WHERE stage=? AND enabled=1',(stage,))}
                def check_pool(pool):
                    self.assertEqual({ident for ident,payload in pool},expected)
                    self.assertGreater(len({payload['category'] for ident,payload in pool}),1)
                    return pool[0]
                with patch.object(hm.RNG,'choice',side_effect=check_pool):
                    hm.choose(stage,[])
                questions=[(row['id'],json.loads(row['payload'])) for row in conn.execute(
                    'SELECT id,payload FROM game_questions WHERE stage=? AND enabled=1',(stage,))]
                blocked={payload['category'] for _,payload in questions[:3]}
                expected={ident for ident,payload in questions if payload['category'] not in blocked}
                with patch.object(hm.RNG,'choice',side_effect=check_pool):
                    hm.choose(stage,[],blocked)
                with self.assertRaises(hm.GameError):
                    hm.choose(stage,[],{payload['category'] for _,payload in questions})

    def test_fifteen_questions_have_no_adjacent_repeated_categories_across_levels(self):
        self.register()
        # Prefer the earliest eligible row, so category filtering must do the work.
        with patch.object(hm.RNG,'choice',side_effect=lambda pool:pool[0]):
            self.start()
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            questions=[json.loads(row[0]) for row in conn.execute(
                'SELECT question_json FROM game_answers WHERE session_id=? ORDER BY position',(self.state['id'],))]
        self.assertEqual([q['stage'] for q in questions],list(hm.STAGES))
        for index,(previous,current) in enumerate(zip(questions,questions[1:]),2):
            self.assertNotEqual(previous['category'],current['category'],f'Questions {index-1} and {index}')

    def test_change_question_avoids_both_neighbors_and_keeps_other_snapshots(self):
        self.register()
        for position in (1,2,5,6,7,10,11,13,14,15):
            with self.subTest(position=position):
                self.clock+=65
                result=self.post('start',dict(request_id=f'category-change-test-{position}'))
                self.assertEqual(result.status_code,200,result.get_json());self.state=result.get_json()['state']
                self.advance(position-1)
                with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
                    before=conn.execute('SELECT position,question_json FROM game_answers WHERE session_id=? ORDER BY position',
                                        (self.state['id'],)).fetchall()
                    bank_before=[]
                    # Simulate a bank edit: neighbors' stored categories remain authoritative.
                    for neighbor in (position-1,position+1):
                        if 1<=neighbor<=15:
                            payload=json.loads(before[neighbor-1][1])
                            bank_before.append((conn.execute('SELECT payload FROM game_questions WHERE id=?',(payload['id'],)).fetchone()[0],payload['id']))
                            payload['category']='Edited bank category'
                            conn.execute('UPDATE game_questions SET payload=? WHERE id=?',(json.dumps(payload),payload['id']))
                    conn.commit()
                blocked={json.loads(payload)['category'] for number,payload in before if abs(number-position)<=1}
                # Prefer a forbidden category if filtering accidentally leaves one available.
                with patch.object(hm.RNG,'choice',side_effect=lambda pool:next((item for item in pool if item[1]['category'] in blocked),pool[0])):
                    self.assertEqual(self.action('change').status_code,200)
                with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
                    after=conn.execute('SELECT position,question_json FROM game_answers WHERE session_id=? ORDER BY position',
                                       (self.state['id'],)).fetchall()
                    conn.executemany('UPDATE game_questions SET payload=? WHERE id=?',bank_before)
                    conn.commit()
                self.assertEqual(before[:position-1]+before[position:],after[:position-1]+after[position:])
                replacement=json.loads(after[position-1][1])
                self.assertEqual(replacement['stage'],hm.STAGES[position-1])
                for neighbor in (position-1,position,position+1):
                    if 1<=neighbor<=15:
                        self.assertNotEqual(replacement['category'],json.loads(before[neighbor-1][1])['category'])
                self.assertEqual(self.action('walk').status_code,200)

    def test_related_character_questions_cannot_recur_across_levels_or_change(self):
        questions=json.loads((ROOT/'heavens_millionaire_questions.json').read_text(encoding='utf-8'))['questions']
        bank=json.loads((ROOT/'heavens_millionaire_questions.json').read_text(encoding='utf-8'))
        keys=bank['repeat_keys_by_id']
        first=next(q for q in questions if q.get('question_family') and any(
            other.get('question_family')==q['question_family'] and other['stage']!=q['stage'] for other in questions))
        for stage in set(hm.STAGES):
            expected={q['id'] for q in questions if q['stage']==stage and q['id']!=first['id']
                      and q.get('question_family')!=first['question_family']
                      and not set(keys[q['id']]) & set(keys[first['id']])}
            with self.app.app_context():
                def check_pool(pool):
                    self.assertEqual({ident for ident,payload in pool},expected)
                    return pool[0]
                with patch.object(hm.RNG,'choice',side_effect=check_pool):
                    hm.choose(stage,[first['id']])
        self.register();self.start();self.action('change')
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            used=json.loads(conn.execute('SELECT seen_json FROM game_sessions WHERE id=?',(self.state['id'],)).fetchone()[0])
        families=[q['question_family'] for q in questions if q['id'] in used and q.get('question_family')]
        self.assertEqual(len(families),len(set(families)))

    def test_new_books_have_valid_source_keys_and_difficulty_ratings(self):
        from collections import Counter
        bank=json.loads((ROOT/'heavens_millionaire_questions.json').read_text(encoding='utf-8'))
        added=[q for q in bank['questions'] if hm.question_pool(q)=='quiz_books']
        self.assertEqual(len(added),1782)
        self.assertEqual(Counter(q['source_detail']['author'] for q in added),{'James Stuart Bell':350,'Carl S. Shoup':1432})
        for q in added:
            self.assertEqual(q['difficulty_rating'],list(dict.fromkeys(hm.STAGES)).index(q['stage'])+1)
            self.assertTrue(q['difficulty_reason']);self.assertTrue(q['source_detail']['answer_key_path'])
            self.assertNotIn('all of the above',[a.lower() for a in q['options']])
        keyed={q['id']:q for q in added}
        self.assertEqual(keyed['QB-SHOUP-0877']['answer'],'Esau')
        self.assertEqual(keyed['QB-SHOUP-0883']['answer'],'the Lord spoke to it')

    def test_every_game_has_six_new_nine_previous_and_no_lifetime_repeats(self):
        from collections import Counter
        self.register();owner=self.account['player']['id'];seen=set()
        for number in range(5):
            with self.subTest(game=number):
                self.clock+=65
                response=self.post('start',dict(request_id=f'source-ratio-game-{number}'))
                self.assertEqual(response.status_code,200,response.get_json());self.state=response.get_json()['state']
                with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
                    questions=[json.loads(r[0]) for r in conn.execute('SELECT question_json FROM game_answers WHERE session_id=? ORDER BY position',(self.state['id'],))]
                    for q in questions:
                        keys={r[0] for r in conn.execute('SELECT repeat_key FROM game_question_repeat_keys WHERE question_id=?',(q['id'],))}
                        self.assertFalse(keys & seen,q['id'])
                self.assertEqual(Counter(hm.question_pool(q) for q in questions),{'quiz_books':6,'previous':9})
                original_pool=hm.question_pool(questions[0])
                self.assertEqual(self.action('change').status_code,200)
                with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
                    changed=[json.loads(r[0]) for r in conn.execute('SELECT question_json FROM game_answers WHERE session_id=? ORDER BY position',(self.state['id'],))]
                self.assertEqual(hm.question_pool(changed[0]),original_pool)
                self.assertEqual(Counter(hm.question_pool(q) for q in changed),{'quiz_books':6,'previous':9})
                self.clock+=3;self.advance(15)
                self.assertEqual(self.state['status'],'won')
                with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
                    seen={r[0] for r in conn.execute('SELECT repeat_key FROM game_player_question_history WHERE player_id=?',(owner,))}
                self.assertIn('id:'+questions[0]['id'],seen)
                self.assertIn('id:'+changed[0]['id'],seen)

    def test_history_survives_restart_and_recovery_and_is_private_to_each_player(self):
        self.register();owner=self.account['player']['id'];self.start()
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            questions=[json.loads(r[0]) for r in conn.execute('SELECT question_json FROM game_answers WHERE session_id=? ORDER BY position',(self.state['id'],))]
        self.action('walk')
        payload=dict(player_code=self.account['player']['player_code'],recovery_code=self.account['recovery_code'],password='a-recovered-password')
        self.assertEqual(self.post('recover',payload).status_code,200)
        hm.initialize(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'],ROOT/'heavens_millionaire_questions.json')
        with self.app.app_context():
            self.assertNotIn(questions[0]['id'],{i for i,q in hm.available_questions(questions[0]['stage'],[],player_id=owner)})
            self.assertIn(questions[1]['id'],{i for i,q in hm.available_questions(questions[1]['stage'],[],player_id=owner)})
        self.register();another=self.account['player']['id']
        with self.app.app_context():
            self.assertIn(questions[0]['id'],{i for i,q in hm.available_questions(questions[0]['stage'],[],player_id=another)})

    def test_history_migration_recovers_shown_and_replaced_but_not_unreached_questions(self):
        self.register();owner=self.account['player']['id'];self.start()
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            original=json.loads(conn.execute('SELECT question_json FROM game_answers WHERE token=?',(self.state['token'],)).fetchone()[0])
        self.action('change')
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            replacement=json.loads(conn.execute('SELECT question_json FROM game_answers WHERE token=?',(self.state['token'],)).fetchone()[0])
            conn.execute('DELETE FROM game_player_question_history');conn.execute("DELETE FROM game_settings WHERE key='question_history_v1'");conn.commit()
        hm.initialize(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'],ROOT/'heavens_millionaire_questions.json')
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            ids={r[0] for r in conn.execute("SELECT repeat_key FROM game_player_question_history WHERE player_id=? AND repeat_key LIKE 'id:%'",(owner,))}
        self.assertEqual(ids,{'id:'+original['id'],'id:'+replacement['id']})

    def test_exhausted_source_pool_never_recycles_questions_or_creates_partial_game(self):
        self.register();owner=self.account['player']['id'];self.start();self.action('walk')
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            conn.executemany('INSERT OR IGNORE INTO game_player_question_history VALUES(?,?,?)',
                             [(owner,'id:'+q['id'],self.clock) for q in json.loads((ROOT/'heavens_millionaire_questions.json').read_text(encoding='utf-8'))['questions']])
            conn.commit();before=conn.execute('SELECT count(*) FROM game_sessions').fetchone()[0]
        response=self.post('start',dict(request_id='exhausted-source-pool-game'))
        self.assertEqual(response.status_code,409);self.assertIn('unseen',response.get_json()['error'])
        with closing(sqlite3.connect(self.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM game_sessions').fetchone()[0],before)

    def test_same_scripture_fact_is_excluded_across_books_and_previous_questions(self):
        self.register();owner=self.account['player']['id']
        with self.app.app_context():
            conn=hm.db()
            pairs=conn.execute('''SELECT a.question_id,b.question_id FROM game_question_repeat_keys a
                JOIN game_question_repeat_keys b ON b.repeat_key=a.repeat_key
                WHERE a.repeat_key LIKE 'fact:%' AND a.question_id NOT LIKE 'QB-%'
                AND b.question_id LIKE 'QB-%' LIMIT 1''').fetchone()
            self.assertIsNotNone(pairs)
            previous=json.loads(conn.execute('SELECT payload FROM game_questions WHERE id=?',(pairs[0],)).fetchone()[0])
            new=json.loads(conn.execute('SELECT payload FROM game_questions WHERE id=?',(pairs[1],)).fetchone()[0])
            with hm.transaction():hm.remember_question(conn,owner,previous,self.clock)
            eligible={ident for ident,q in hm.available_questions(new['stage'],[],player_id=owner)}
            self.assertNotIn(new['id'],eligible)


class WebsiteIntegrationTests(unittest.TestCase):
    def test_real_website_game_routes_never_sync_google_or_write_ministry_records(self):
        with tempfile.TemporaryDirectory() as directory:
            env=dict(os.environ,DATA_DIR=directory,AI_DATABASE=str(Path(directory)/'ai.db'),BIBLE_STUDY_DATABASE=str(Path(directory)/'bible.db'),GEMINI_API_KEY='offline-key',FLASK_SECRET_KEY='offline-session-key')
            script='''import app
from unittest.mock import patch
client=app.app.test_client()
with patch.object(app,'sync_from_sheets_if_needed',side_effect=AssertionError('Game called Sheets')),patch.object(app,'_log_visit_if_needed',side_effect=AssertionError('Game logged ministry visit')):
    for path in ('/bible-games','/bible-games/api/bootstrap','/static/heavens_millionaire/game.js','/static/heavens_millionaire/music-1.mp3'):
        result=client.get(path)
        assert result.status_code==200,(path,result.status_code)
        result.close()
print('INTEGRATION_READY')
'''
            result=subprocess.run([sys.executable,'-B','-c',script],cwd=ROOT,env=env,capture_output=True,text=True,timeout=90)
            self.assertEqual(result.returncode,0,result.stderr);self.assertIn('INTEGRATION_READY',result.stdout)


@unittest.skipUnless(os.getenv('HEAVENS_BROWSER_TESTS') == '1', 'Set HEAVENS_BROWSER_TESTS=1 for Chromium and Safari/WebKit checks')
class GameBrowserTests(unittest.TestCase):
    def test_website_accounts_go_directly_to_photo_without_church_fields(self):
        import threading
        from werkzeug.serving import make_server
        from playwright.sync_api import sync_playwright
        fixture=MillionaireTests();fixture.setUp();fixture.app.config.pop('HEAVENS_MILLIONAIRE_CLOCK')
        server=make_server('127.0.0.1',0,fixture.app,threaded=True)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        folder=ROOT/'.cache_optimization_work/heavens_millionaire';folder.mkdir(parents=True,exist_ok=True)
        try:
            with sync_playwright() as browsers:
                cases=((browsers.chromium,'pastor','Test Church'),(browsers.webkit,'overseer','Area Overseer'))
                for engine,username,affiliation in cases:
                    site_client=fixture.app.test_client()
                    with site_client.session_transaction() as signed:
                        if username=='pastor':
                            signed['pastor_logged_in']=True;signed['pastor_username']=username
                        else:
                            signed['ao_logged_in']=True;signed['ao_username']=username
                    browser=engine.launch(headless=True)
                    context=browser.new_context(viewport=dict(width=768,height=1024),has_touch=True,is_mobile=True)
                    context.add_cookies([dict(name='session',value=site_client.get_cookie('session').value,domain='127.0.0.1',path='/',httpOnly=True,sameSite='Lax')])
                    page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                    url=f'http://127.0.0.1:{server.server_port}/bible-games'
                    page.goto(url,wait_until='networkidle');page.locator('#photo-form').wait_for()
                    self.assertEqual(page.locator('#area,#church,input[type=password]').count(),0)
                    page.locator('#photo').set_input_files(dict(name='portrait.jpg',mimeType='image/jpeg',buffer=fixture.photo().getvalue()))
                    self.assertTrue(page.locator('#photo-preview').is_visible())
                    page.locator('[name=consent]').check()
                    page.screenshot(path=str(folder/f'linked-photo-{username}-ipad.png'),full_page=True)
                    with page.expect_response(lambda response:response.url.endswith('/api/photo')) as response:
                        page.locator('#photo-form [type=submit]').click()
                    self.assertEqual(response.value.status,200)
                    saved=response.value.json()['player']
                    self.assertTrue(saved['linked']);self.assertEqual(saved['church'],affiliation)
                    self.assertEqual(saved['pastor'],'Test Pastor' if username=='pastor' else '')
                    page.locator('#enter-studio').wait_for(timeout=15000);page.locator('#enter-studio').click()
                    page.locator('header [data-nav=profile]').click()
                    page.get_by_role('button',name='Update portrait',exact=True).click()
                    self.assertEqual(page.locator('#area,#church').count(),0)
                    page.reload(wait_until='networkidle');page.locator('#start-game').wait_for()
                    with page.expect_response(lambda response:response.url.endswith('/api/start')) as started:
                        page.locator('#start-game').click()
                    self.assertEqual(started.value.status,200)
                    page.locator('[data-walk]').click();page.locator('#modal [data-action=walk]').click()
                    page.get_by_role('heading',name='You knew when to walk away.').wait_for()
                    self.assertEqual(errors,[])
                    context.close();browser.close()
        finally:
            server.shutdown();thread.join(timeout=5);server.server_close();fixture.doCleanups()

    def test_registration_loading_music_gameplay_and_ipad_rotation(self):
        import threading
        from werkzeug.serving import make_server
        from playwright.sync_api import sync_playwright
        fixture=MillionaireTests();fixture.setUp();fixture.app.config.pop('HEAVENS_MILLIONAIRE_CLOCK')
        server=make_server('127.0.0.1',0,fixture.app,threaded=True)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        folder=ROOT/'.cache_optimization_work/heavens_millionaire';folder.mkdir(parents=True,exist_ok=True)
        try:
            with sync_playwright() as browsers:
                chrome=browsers.chromium.launch(headless=True)
                context=chrome.new_context(viewport=dict(width=390,height=844),has_touch=True,is_mobile=True)
                page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                url=f'http://127.0.0.1:{server.server_port}/bible-games'
                page.goto(url,wait_until='networkidle')
                self.assertEqual(page.title(),'Who Wants to Be Heaven\u2019s Millionaire?')
                page.screenshot(path=str(folder/'welcome-phone.png'),full_page=True)
                page.get_by_role('button',name='Take the challenge').click()
                page.locator('#name').fill('Browser Player')
                page.locator('#area').select_option('1')
                church=next(c for c in fixture.bootstrap()['directory'] if c['area']=='1')
                page.locator('#church').select_option(church['id'])
                self.assertIn('Test Pastor',page.locator('#pastor').inner_text())
                page.locator('#password').fill('browser-test-password')
                page.locator('#photo').set_input_files(dict(name='portrait.jpg',mimeType='image/jpeg',buffer=fixture.photo().getvalue()))
                page.locator('[name=consent]').check()
                page.get_by_role('button',name='Create my player account').click()
                page.locator('.credential code').first.wait_for()
                code=page.locator('.credential code').first.inner_text()
                page.locator('#continue').click()
                page.locator('#enter-studio').wait_for(timeout=15000)
                self.assertEqual(page.locator('.progress-track').get_attribute('aria-valuenow'),'100')
                page.screenshot(path=str(folder/'loading-phone.png'),full_page=True)
                page.locator('#enter-studio').click()
                with page.expect_response(lambda r:r.url.endswith('/api/start')) as response:
                    page.locator('#start-game').click()
                state=response.value.json()['state'];page.locator('.question-text').wait_for()
                self.assertNotIn('correct_choice',state)
                page.screenshot(path=str(folder/'question-phone.png'),full_page=True)
                self.assertFalse(page.evaluate('document.documentElement.scrollWidth>innerWidth'))
                self.assertGreaterEqual(page.locator('.answer').first.bounding_box()['height'],44)
                page.locator('[data-action=fifty]').click()
                page.wait_for_function('document.querySelectorAll(".answer.eliminated").length===2')
                page.wait_for_function('!document.querySelector("#modal").open',timeout=6000)
                self.assertEqual(page.locator('.answer.eliminated').count(),2)
                with closing(sqlite3.connect(fixture.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
                    correct=conn.execute('SELECT correct_choice FROM game_answers WHERE token=?',(state['token'],)).fetchone()[0]
                page.locator(f'[data-choice="{correct}"]').click();page.locator('#confirm-answer').click()
                page.locator('[data-action=next]').wait_for();page.locator('[data-action=next]').click();page.locator('.question-text').wait_for()
                page.locator('[data-walk]').click();page.locator('#modal [data-action=walk]').click()
                page.get_by_role('heading',name='You knew when to walk away.').wait_for()
                page.locator('header [data-nav=leaderboard]').click();page.locator('tbody .player-cell').first.wait_for()
                self.assertIn('Browser Player',page.locator('tbody').inner_text())
                self.assertTrue(page.locator('tbody img').first.evaluate('image=>image.complete&&image.naturalWidth===320'))
                page.screenshot(path=str(folder/'scoreboard-phone.png'),full_page=True)
                self.assertEqual(errors,[])
                context.close();chrome.close()

                for engine,name in ((browsers.chromium,'chromium'),(browsers.webkit,'webkit')):
                    browser=engine.launch(headless=True)
                    context=browser.new_context(viewport=dict(width=768,height=1024),has_touch=True,is_mobile=True)
                    page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                    page.goto(url,wait_until='networkidle')
                    page.screenshot(path=str(folder/f'welcome-ipad-{name}.png'),full_page=True)
                    page.get_by_role('button',name='Returning player').click()
                    page.locator('#player-code').fill(code);page.locator('#password').fill('browser-test-password')
                    page.locator('#auth-form [type=submit]').click()
                    page.locator('#enter-studio').wait_for(timeout=15000);page.locator('#enter-studio').click()
                    with page.expect_response(lambda r:r.url.endswith('/api/start')) as response:
                        page.locator('#start-game').click()
                    original=response.value.json()['state'];page.locator('.question-text').wait_for()
                    for orientation,width,height in (('portrait',768,1024),('landscape',1024,768)):
                        page.set_viewport_size(dict(width=width,height=height))
                        page.screenshot(path=str(folder/f'question-ipad-{name}-{orientation}.png'),full_page=True)
                        self.assertFalse(page.evaluate('document.documentElement.scrollWidth>innerWidth'))
                        self.assertTrue(page.locator('.prize-ladder').is_visible())
                        self.assertGreaterEqual(page.locator('.answer').first.bounding_box()['height'],44)
                    page.reload(wait_until='networkidle');page.locator('.question-text').wait_for()
                    refreshed=page.evaluate('(id)=>fetch("/bible-games/api/state/"+id).then(r=>r.json())',original['id'])['state']
                    self.assertEqual(refreshed['deadline'],original['deadline'])
                    self.assertEqual(refreshed['interruptions'],0)
                    if name=='chromium':
                        # Exercise the harder-stage music through real server actions.
                        csrf=page.evaluate('fetch("/bible-games/api/bootstrap").then(r=>r.json())')['csrf']
                        active=refreshed
                        for _ in range(10):
                            with closing(sqlite3.connect(fixture.app.config['HEAVENS_MILLIONAIRE_DATABASE'])) as conn:
                                correct=conn.execute('SELECT correct_choice FROM game_answers WHERE token=?',(active['token'],)).fetchone()[0]
                            payload=dict(session_id=active['id'],token=active['token'],action='answer',choice=correct)
                            response=page.request.post(url+'/api/action',data=payload,headers={'X-CSRF-Token':csrf})
                            self.assertEqual(response.status,200);active=response.json()['state']
                            response=page.request.post(url+'/api/action',data=dict(session_id=active['id'],token=active['token'],action='next'),headers={'X-CSRF-Token':csrf})
                            self.assertEqual(response.status,200);active=response.json()['state']
                        with page.expect_request(lambda request:request.url.endswith('/music-2.mp3')):
                            page.reload(wait_until='networkidle')
                        page.locator('.stage-pill').filter(has_text='Expert').wait_for()
                    if name=='webkit':
                        # Multiple signals for one away episode produce only one warning.
                        page.evaluate("window.dispatchEvent(new Event('blur'))")
                        page.wait_for_timeout(1100)
                        page.evaluate("window.dispatchEvent(new Event('blur'))")
                        page.wait_for_timeout(2100)
                        page.evaluate("window.dispatchEvent(new Event('focus'))")
                        checked=page.evaluate('(id)=>fetch("/bible-games/api/state/"+id).then(r=>r.json())',original['id'])['state']
                        self.assertEqual(checked['interruptions'],1)
                        page.evaluate("window.dispatchEvent(new Event('blur'))")
                        page.get_by_role('heading',name='Attempt disqualified under competition rules.').wait_for()
                    else:
                        page.locator('[data-walk]').click();page.locator('#modal [data-action=walk]').click()
                        page.get_by_role('heading',name='You knew when to walk away.').wait_for()
                    self.assertEqual(errors,[])
                    context.close();browser.close()
        finally:
            server.shutdown();thread.join(timeout=5);server.server_close();fixture.doCleanups()


if __name__=='__main__':
    unittest.main()
