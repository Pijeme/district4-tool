"""Offline browser checks for library controls and EPUB visual zoom.

Uses an installed Chromium browser, temporary HTML and mocked API responses.
No application server, Google access or production databases are started.
"""

from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from flask import Flask
from jinja2 import DictLoader

import pastor_resources as resources


ROOT = Path(__file__).resolve().parents[1]
BROWSER = next((str(path) for path in (
    Path(r'C:\Program Files\Google\Chrome\Application\chrome.exe'),
    Path(r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'),
) if path.is_file()), None) or shutil.which('chromium') or shutil.which('google-chrome')


class ResultParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = False
        self.value = ''

    def handle_starttag(self, tag, attrs):
        if tag == 'pre' and ('id', 'browser-test-result') in attrs:
            self.active = True

    def handle_endtag(self, tag):
        if tag == 'pre':
            self.active = False

    def handle_data(self, data):
        if self.active:
            self.value += data


@unittest.skipUnless(BROWSER, 'An installed Chromium browser is required')
class LibraryBrowserTests(unittest.TestCase):
    def run_browser(self, html, verify, setup='', window_size=(1100, 850)):
        setup = '''window.browserErrors=[];
        window.addEventListener('error', e=>{if(e.message)browserErrors.push(e.message)});
        window.addEventListener('unhandledrejection', e=>browserErrors.push(String(e.reason)));
        function check(value,message){if(!value)throw Error(message)};
        ''' + setup
        verify = '''setTimeout(async()=>{
            const result=document.createElement('pre');result.id='browser-test-result';document.body.appendChild(result);
            try{''' + verify + '''
                check(!browserErrors.length,browserErrors.join('; '));result.textContent='PASS';
            }catch(error){result.textContent='FAIL: '+error.stack}
        },150);'''
        html = html.replace('<head>', '<head><script>' + setup + '</script>')
        html = html.replace('</body>', '<script>' + verify + '</script></body>')
        # Load only repository scripts; the tests never fetch CDN assets.
        def local_script(match):
            filename = match.group(1).split('?', 1)[0].rsplit('/', 1)[-1]
            if filename not in ('epub_page_view.js', 'resource_file_actions.js', 'bible_study.js', 'bulletin_insight.js'):
                return ''
            source = (ROOT / 'static' / filename).read_text(encoding='utf-8')
            return '<script>' + source.replace('</script', r'<\/script') + '</script>'
        html = re.sub(r'<script src="([^"]+)"[^>]*></script>', local_script, html)
        html = re.sub(r'@import url\([^;]+;', '', html)
        with tempfile.TemporaryDirectory(prefix='district4_browser_') as directory:
            path = Path(directory) / 'test.html'
            path.write_text(html, encoding='utf-8')
            command = [BROWSER, '--headless', '--disable-gpu', '--no-first-run',
                       '--no-default-browser-check', '--allow-file-access-from-files',
                       '--user-data-dir=' + str(Path(directory) / 'profile'),
                       '--dump-dom', '--virtual-time-budget=4000',
                       '--window-size=' + ','.join(map(str, window_size)), path.as_uri()]
            if os.name != 'nt' and os.geteuid() == 0:
                command.insert(1, '--no-sandbox')
            process = subprocess.run(command, capture_output=True, text=True, encoding='utf-8',
                                     errors='replace', timeout=40,
                                     creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
            parser = ResultParser()
            parser.feed(process.stdout)
            self.assertEqual(parser.value, 'PASS', parser.value or process.stderr[-2000:])

    def render_library(self, admin):
        app = Flask(__name__)
        with patch.object(resources, 'ensure_v3_tables'):
            resources.register_pastor_resources_routes(app)
        app.add_url_rule('/sermon-ebooks', endpoint='sermon_ebooks_home', view_func=lambda: '')
        app.jinja_loader = DictLoader({'base.html': '<html><head></head><body>{% block content %}{% endblock %}</body></html>'})
        with app.test_request_context():
            return app.jinja_env.from_string(resources.PASTOR_RESOURCES_V3_HTML).render(is_admin=admin)

    def library_setup(self):
        books = [dict(id=42, title='Zulu Newest', author='Author', category='Prayer', favorite=False,
                      recommended=False, page_count=10, indexed_page_count=10, manual_page_count=250),
                 dict(id=43, title='Alpha Older', author='Author', category='Prayer', favorite=False,
                      recommended=False, page_count=0, indexed_page_count=0, manual_page_count=None),
                 dict(id=44, title='Shelf Only', author='Author', category='Prayer', favorite=False,
                      recommended=True, page_count=0, indexed_page_count=0, manual_page_count=None)]
        for book in books:
            book.update(thumbnail_url='data:image/gif;base64,R0lGODlhAQABAAAAACw=',
                        read_url='/read/' + str(book['id']), download_url='/download/' + str(book['id']))
        return 'window.fixtureBooks=' + json.dumps(books) + ''';
        window.fixtureRequests=[];
        window.fetch=async(url,options={})=>{
            fixtureRequests.push(String(url));
            const parsed=new URL(String(url),'https://example.test');
            let result={ok:true,state:{running:false,stage:'idle'},books:[],pages:1,page:1,total:0};
            if(parsed.pathname.endsWith('/api/books')){
                let books=parsed.searchParams.get('mode')==='recommended'
                    ? fixtureBooks.filter(b=>b.recommended) : fixtureBooks.filter(b=>b.id!==44);
                if(parsed.searchParams.get('mode')==='alphabetical')books=[...books].sort((a,b)=>a.title.localeCompare(b.title));
                result={...result,books,total:books.length,stats:{last_sync_at:'2026-10-20T00:00:00Z'}};
            }else if(parsed.pathname.includes('/admin/recommend/')){
                const book=fixtureBooks.find(b=>b.id===Number(parsed.pathname.split('/').pop()));
                book.recommended=JSON.parse(options.body).recommended;result.recommended=book.recommended;
            }else if(parsed.pathname.includes('/admin/edit/')){
                const book=fixtureBooks.find(b=>b.id===Number(parsed.pathname.split('/').pop()));
                const update=JSON.parse(options.body);Object.assign(book,update);
                book.manual_page_count=update.manual_page_count?Number(update.manual_page_count):null;
                book.page_count=book.indexed_page_count||book.manual_page_count||0;
            }else if(parsed.pathname.includes('/api/favorite/')){
                fixtureBooks.find(b=>b.id===Number(parsed.pathname.split('/').pop())).favorite=JSON.parse(options.body).favorite;
            }
            return new Response(JSON.stringify(result),{headers:{'Content-Type':'application/json'}});
        };'''

    def test_developer_library_recommendation_edit_sort_and_favorites(self):
        self.run_browser(self.render_library(True), '''
            check(currentBookMap.size===2,'Main library did not initialize');
            check(recommendedBookMap.size===1,'Recommendation shelf did not initialize');
            check(document.querySelector('[data-mode="alphabetical"]'),'Title A-Z missing');
            check(!document.querySelector('[data-mode="recent"]'),'Old Recently Added button remains');
            const firstMain=document.querySelector('#bookGrid .pr-book-title').textContent;
            check(firstMain==='Zulu Newest','Newest-first main view missing');
            const recommend=document.querySelector('#bookGrid [data-admin-action="recommend"][data-book-id="42"]');
            await toggleRecommendation(42,recommend);
            check(recommendedBookMap.size===2,'Developer recommendation did not appear');
            const ids=[...document.querySelectorAll('[id]')].map(e=>e.id);
            check(new Set(ids).size===ids.length,'Book cards have duplicate IDs');
            const favorite=document.querySelector('#bookGrid [data-book-card="42"] .pr-fav');
            await toggleFavorite(42,favorite);
            check([...document.querySelectorAll('[data-book-card="42"] .pr-fav')].every(b=>b.classList.contains('on')),'Favorite state differs between shelf and library');
            openEditModalById(44);
            document.getElementById('editPageCount').value='73';
            await saveBookEdit();
            check(document.querySelector('#recommendedGrid [data-book-card="44"] .pr-pages').textContent==='73 pages','Manual count not displayed on recommendation');
            openEditModalById(42);
            check(document.getElementById('editPageCount').value==='250','Saved fallback was replaced with indexed count');
            check(document.getElementById('editPageCountHelp').textContent.includes('10 pages'),'Index priority is not explained');
            closeEditModal();
            await changeMode('alphabetical',document.querySelector('[data-mode="alphabetical"]'));
            check(document.querySelector('#bookGrid .pr-book-title').textContent==='Alpha Older','Title sorting failed');
            const remove=document.querySelector('#recommendedGrid [data-admin-action="recommend"][data-book-id="42"]');
            await toggleRecommendation(42,remove);
            check(!recommendedBookMap.has(42),'Recommendation was not removed');
            check(fixtureBooks.find(b=>b.id===42).favorite,'Removing recommendation cleared personal favorite');
        ''', self.library_setup())

    def test_reader_sees_recommendations_without_developer_controls(self):
        self.run_browser(self.render_library(False), '''
            check(document.getElementById('recommendedSection').style.display==='block','Public recommendations are hidden');
            check(document.querySelector('#recommendedGrid .pr-book-title').textContent==='Shelf Only','Wrong recommendation');
            check(!document.querySelector('[data-admin-action]'),'Developer controls leaked to reader');
            check(!document.getElementById('editModalBackdrop'),'Developer edit form leaked to reader');
        ''', self.library_setup())

    def test_epub_zoom_pinch_pan_fit_preserve_layout_and_reading_position(self):
        html = '''<html><head></head><body>
        <div id="viewport" style="position:relative;width:500px;height:360px;overflow:hidden">
          <div id="stage" style="width:100%;height:100%;transform-origin:0 0">
            <iframe id="content" style="border:0;width:100%;height:100%"></iframe>
          </div>
        </div><script src="/static/epub_page_view.js"></script></body></html>'''
        self.run_browser(html, '''
            const viewport=document.getElementById('viewport'),stage=document.getElementById('stage');
            const frame=document.getElementById('content'),doc=frame.contentDocument;
            doc.body.innerHTML='<p style="font:20px/1.5 serif">'+('Text remains on the same page. '.repeat(40))+'</p><mark>Saved highlight</mark>';
            const paragraph=doc.querySelector('p'),mark=doc.querySelector('mark');
            const height=paragraph.offsetHeight,width=paragraph.offsetWidth;
            let selection=false;
            const view=createEpubPageView({viewport,stage,selectionActive:()=>selection});view.install(doc);
            view.zoomBy(1);
            check(view.getScale()===2,'Zoom button failed');
            check(doc.documentElement.style.touchAction==='none','Native iframe scrolling competes with zoomed panning');
            check(paragraph.offsetHeight===height&&paragraph.offsetWidth===width,'Zoom reflowed EPUB text');
            check(doc.querySelector('mark')===mark,'Zoom replaced an annotation node');
            check(view.blocksNavigation(),'Zoom permits accidental swipe navigation');
            function touch(type,points){
                const e=new Event(type,{bubbles:true,cancelable:true});
                Object.defineProperty(e,'touches',{value:points.map(([x,y])=>({clientX:x,clientY:y}))});
                doc.dispatchEvent(e);return e;
            }
            view.fit();
            touch('touchstart',[[100,100],[200,100]]);
            touch('touchmove',[[70,100],[230,100]]);
            check(Math.abs(view.getScale()-1.6)<0.01,'Iframe pinch did not magnify the page');
            touch('touchend',[[70,100]]);touch('touchend',[]);
            check(view.blocksNavigation(),'Pinch-end is allowed to turn a page');
            const before=stage.style.transform;
            touch('touchstart',[[100,100]]);touch('touchmove',[[75,75]]);touch('touchend',[]);
            check(stage.style.transform!==before,'Zoomed page cannot pan');
            selection=true;
            const selectedBefore=stage.style.transform;
            touch('touchstart',[[100,100]]);touch('touchmove',[[80,80]]);touch('touchend',[]);
            check(stage.style.transform===selectedBefore,'Panning interferes with an active text selection');
            selection=false;view.fit();
            check(view.getScale()===1,'Fit Page did not reset zoom');
            check(doc.documentElement.style.touchAction==='','Fit Page did not restore native touch behavior');
            check(paragraph.offsetHeight===height&&doc.querySelector('mark')===mark,'Fit Page changed text or highlights');
            frame.style.display='none';
            const direct=document.createElement('div');direct.style.cssText='height:100%;overflow-y:auto;scrollbar-gutter:stable';
            direct.innerHTML='<article style="height:2000px">iOS chapter content</article>';stage.appendChild(direct);
            direct.scrollTop=123;const scrollHeight=direct.scrollHeight;
            view.zoomBy(1);
            check(direct.scrollTop===123&&direct.scrollHeight===scrollHeight,'Visual zoom changed direct-reader position');
            view.fit();check(direct.scrollTop===123,'Fit Page lost direct-reader position');
        ''')


@unittest.skipUnless(BROWSER, 'An installed Chromium browser is required')
class BibleStudyBrowserTests(unittest.TestCase):
    run_browser = LibraryBrowserTests.run_browser

    def render_preview(self, admin=False):
        import bible_study
        from jinja2 import ChoiceLoader, FileSystemLoader
        app = Flask(__name__)
        app.secret_key = 'isolated-browser-test-key'
        bible_study.register_bible_study_routes(app)
        app.add_url_rule('/pastor-resources', endpoint='pastor_resources', view_func=lambda: '')
        app.jinja_loader = ChoiceLoader([
            DictLoader({'base.html': '<html><head></head><body class="district-glass"><button id="outsideBible">Outside control</button><main class="app-main">{% block content %}{% endblock %}</main></body></html>'}),
            FileSystemLoader(str(ROOT / 'templates')),
        ])
        with app.test_request_context():
            html = app.jinja_env.get_template('bible_study.html').render(
                csrf_token='test-csrf', bible_study_admin=admin,
                bible_books=[{'name': name, 'chapters': count} for name, count in bible_study.BOOKS])
        baseline = '<style>' + (ROOT / 'static/glass_theme.css').read_text(encoding='utf-8') + '</style>'
        html = html.replace('<head>', '<head>' + baseline)
        return re.sub(r'<link rel="stylesheet"[^>]*bible_study\.css[^>]*>',
                      lambda _match: '<style>' + (ROOT / 'static/bible_study.css').read_text(encoding='utf-8') + '</style>', html)

    def setup_api(self, initialized):
        return 'window.schemaReady=' + json.dumps(initialized) + ''';
        window.bibleRequests=[];window.serverVersion=0;
        window.fetch=async(url,options={})=>{
            bibleRequests.push({url:String(url),options});
            let data={ok:true};
            if(String(url).endsWith('/status'))data={...data,status:schemaReady?'schema_ready':'not_initialized',
                schema_ready:schemaReady,indexed_verses:0,message:'No Bible text has been imported.'};
            else if(options.method==='POST'){
                check(options.headers['X-CSRF-Token']==='test-csrf','CSRF header missing');
                const settings=JSON.parse(options.body);
                check(settings.version===serverVersion,'Overlapping preference saves use stale versions');
                data.version=++serverVersion;
            }else data={...data,storage_available:schemaReady,state:{book:'Genesis',chapter:1,verse:1,
                theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''

    def test_empty_desktop_layout_divider_fullscreen_and_saved_preferences(self):
        self.run_browser(self.render_preview(), '''
            const root=document.getElementById('bibleStudyRoot'),divider=document.getElementById('bsDivider');
            const bible=document.getElementById('bsBible'),study=document.getElementById('bsStudy');
            check(document.getElementById('bsEmpty').textContent.includes('has not been indexed'),'Empty-data message missing');
            check(bible.getBoundingClientRect().left<study.getBoundingClientRect().left,'Desktop panels are not side by side');
            check(divider.getAttribute('aria-orientation')==='vertical','Desktop divider orientation wrong');
            divider.dispatchEvent(new KeyboardEvent('keydown',{key:'ArrowRight',bubbles:true}));
            check(divider.getAttribute('aria-valuenow')==='62','Keyboard divider does not resize panels');
            const captures=new Set();divider.setPointerCapture=id=>captures.add(id);
            divider.hasPointerCapture=id=>captures.has(id);divider.releasePointerCapture=id=>captures.delete(id);
            const layoutRect=document.getElementById('bsLayout').getBoundingClientRect();
            divider.dispatchEvent(new PointerEvent('pointerdown',{pointerId:1,button:0,bubbles:true}));
            divider.dispatchEvent(new PointerEvent('pointermove',{pointerId:1,clientX:layoutRect.left+layoutRect.width*.7,bubbles:true}));
            divider.dispatchEvent(new PointerEvent('pointerup',{pointerId:1,bubbles:true}));
            check(divider.getAttribute('aria-valuenow')==='70'&&!captures.size,'Pointer drag does not resize or release divider');
            const book=document.getElementById('bsBook');book.value='Jude';book.dispatchEvent(new Event('change'));
            check(document.getElementById('bsChapter').options.length===1,'Jude has placeholder chapters');
            document.getElementById('bsNext').click();
            check(book.value==='Revelation'&&document.getElementById('bsChapter').value==='1','Cross-book navigation failed');
            document.getElementById('bsPrevious').click();check(book.value==='Jude','Previous book boundary failed');
            document.getElementById('bsLarger').click();
            check(root.style.getPropertyValue('--bs-font')==='20px','Text-size control failed');
            root.requestFullscreen=undefined;
            root.querySelector('[data-bs-mode="study"]').click();
            check(root.classList.contains('bs-fullscreen')&&getComputedStyle(bible).display==='none','Study fullscreen fallback failed');
            root.dispatchEvent(new KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
            check(root.dataset.mode==='split'&&!root.classList.contains('bs-fullscreen'),'Escape did not restore split view');
            check([...root.querySelectorAll('.bs-personal-tools button')].every(button=>button.disabled),'Unimplemented annotation tools are enabled');
            check(getComputedStyle(document.getElementById('outsideBible')).minHeight!=='38px','Bible CSS changed outside controls');
            check(!document.getElementById('bsVerseDialog').open,'Empty concordance opens a fabricated verse');
            await new Promise(resolve=>setTimeout(resolve,100));
            check(serverVersion>0,'Preferences were not saved');
            check(bibleRequests.every(r=>r.url.startsWith('/pastor-resources/bible-study/api/')),'Preview calls existing readers or AI');
        ''', self.setup_api(True))

    def test_mobile_panels_stack_and_resize_without_storage_writes(self):
        self.run_browser(self.render_preview(), '''
            const root=document.getElementById('bibleStudyRoot'),divider=document.getElementById('bsDivider');
            const bible=document.getElementById('bsBible'),study=document.getElementById('bsStudy');
            check(innerWidth<=700,'Mobile viewport not applied');
            check(study.getBoundingClientRect().top>bible.getBoundingClientRect().top,'Mobile panels do not stack');
            check(divider.getAttribute('aria-orientation')==='horizontal','Mobile divider orientation wrong');
            divider.dispatchEvent(new KeyboardEvent('keydown',{key:'ArrowDown',bubbles:true}));
            check(divider.getAttribute('aria-valuenow')==='62','Mobile divider does not resize vertically');
            const captures=new Set();divider.setPointerCapture=id=>captures.add(id);
            divider.hasPointerCapture=id=>captures.has(id);divider.releasePointerCapture=id=>captures.delete(id);
            const layoutRect=document.getElementById('bsLayout').getBoundingClientRect();
            divider.dispatchEvent(new PointerEvent('pointerdown',{pointerId:2,button:0,bubbles:true}));
            divider.dispatchEvent(new PointerEvent('pointermove',{pointerId:2,clientY:layoutRect.top+layoutRect.height*.45,bubbles:true}));
            divider.dispatchEvent(new PointerEvent('pointerup',{pointerId:2,bubbles:true}));
            check(divider.getAttribute('aria-valuenow')==='45','Mobile drag does not change vertical split');
            root.requestFullscreen=undefined;
            root.querySelector('[data-bs-mode="bible"]').click();
            check(root.dataset.mode==='bible'&&getComputedStyle(study).display==='none','Bible fullscreen failed');
            root.querySelector('[data-bs-mode="split"]').click();
            check(getComputedStyle(study).display!=='none','Split view did not restore study panel');
            const theme=document.getElementById('bsTheme');theme.value='dark';theme.dispatchEvent(new Event('change'));
            check(root.dataset.theme==='dark','Reading theme did not change');
            check(getComputedStyle(document.getElementById('bsBookHeading')).color===getComputedStyle(root).color,'Site theme overrides Bible night headings');
            check(!bibleRequests.some(r=>r.options.method==='POST'),'Missing database triggered writes');
        ''', self.setup_api(False), window_size=(420, 900))

    def test_navigation_is_beside_library_for_authenticated_library_accounts(self):
        import bible_study
        from flask import render_template_string, session
        app = Flask(__name__)
        app.secret_key = 'isolated-navigation-test-key'
        with patch.object(resources, 'ensure_v3_tables'):
            resources.register_pastor_resources_routes(app)
        bible_study.register_bible_study_routes(app)
        app.add_url_rule('/sermon-ebooks', endpoint='sermon_ebooks_home', view_func=lambda: '')
        app.jinja_loader = DictLoader({'base.html': '{% block content %}{% endblock %}'})
        with app.test_request_context():
            self.assertNotIn('>Bible Study</a>', render_template_string(resources.PASTOR_RESOURCES_V3_HTML, is_admin=False))
            for account in (
                dict(ao_logged_in=True, ao_username='Pijeme', ao_role='Area Overseer'),
                dict(ao_logged_in=True, ao_username='AnotherAO', ao_role='Area Overseer'),
                dict(pastor_logged_in=True, pastor_username='Pastor'),
            ):
                session.clear()
                session.update(account)
                enabled = render_template_string(resources.PASTOR_RESOURCES_V3_HTML, is_admin=False)
                links = re.findall(r'<a class="pr-nav-btn[^>]*href="([^"]+)"[^>]*>([^<]+)</a>', enabled)
                library_index = next(index for index, (_, title) in enumerate(links) if title == 'Library')
                self.assertEqual(links[library_index+1], ('/pastor-resources/bible-study', 'Bible Study'))

    def test_developer_sync_dictionary_expansion_reference_preview_and_copy(self):
        setup = '''
        window.synced=false;window.syncPosts=0;window.syncPolls=0;window.copied='';
        Object.defineProperty(navigator,'clipboard',{value:{writeText:async text=>{window.copied=text}},configurable:true});
        window.fetch=async(url,options={})=>{
            const path=String(url);let data={ok:true};
            if(path.endsWith('/sync')){
                if(options.method==='POST'){
                    check(options.headers['X-CSRF-Token']==='test-csrf','Sync missing CSRF');
                    check(options.body==='{}','Sync sends untrusted source input');
                    syncPosts++;syncPolls=0;
                }else if(!syncPosts)data.sync={status:'idle',message:'Ready to sync'};
                else if(syncPolls++===0)data.sync={status:'running',message:'Indexing source ebooks'};
                else{synced=true;data.sync={status:'completed',message:'Sync complete',counts:{niv_verses:1,mounce_entries:1,vine_entries:0},warnings:['Dictionary searches are not NIV alignments.']};}
            }else if(path.endsWith('/status'))data={...data,schema_ready:synced,indexed_verses:synced?1:0,message:synced?'1 NIV verse available':'Not indexed'};
            else if(path.endsWith('/state'))data={...data,storage_available:synced,state:{book:'Genesis',chapter:1,verse:1,theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};
            else if(path.includes('/chapter?'))data.verses=[{id:7,verse:1,text:'Alpha 😀 beta.',heading:'Fixture heading',tokens:[{text:'Alpha',token_index:0,start_offset:0,end_offset:5},{text:'beta',token_index:1,start_offset:8,end_offset:12}]}];
            else if(path.includes('/word/'))data={...data,word:'Alpha',query:'Alpha',verified:!path.endsWith('/1'),original_tokens:[{spelling:'Synthetic original',language:'Greek',source_ref:'Gen.1.1#01',lemma:'Synthetic lemma',transliteration:'fixture',morphology:'fixture',identifiers:[{extended_strong:'G0001',standard_strong:'G1',definition:'Synthetic definition.'}]}],sources:[{source:'mounce',label:'Mounce',available:true,entries:[{id:3,headword:'ALPHA',section:'Expository dictionary',language:'',preview:'Synthetic short preview.'}],next_offset:null},{source:'vine',label:'Vine',available:true,entries:[],next_offset:null}]};
            else if(path.includes('/dictionaries/entry/'))data.entry={id:3,source_title:'Mounce fixture',identifiers:[],blocks:[{type:'paragraph',text:'<img src=x onerror=alert(1)> Genesis 1:1',references:[{start:29,end:40,book:'Genesis',chapter:1,verse:1}]}]};
            else if(path.includes('/verse?'))data.verse={reference:'Genesis 1:1',text:'Alpha 😀 beta.',attribution:'Synthetic fixture'};
            else throw Error('Unexpected request '+path);
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''
        self.run_browser(self.render_preview(admin=True), '''
            const sync=document.getElementById('bsSync');
            check(sync&&!sync.disabled,'Developer sync button unavailable');
            check(sync.closest('.bs-header-actions'),'Sync is not beside the layout buttons');
            check(!document.querySelector('.bs-preview'),'Removed status strip remains');
            check(document.getElementById('bsSyncReport').closest('#bsSyncDialog'),'Sync report still occupies reader space');
            document.getElementById('bsSyncDetails').click();
            check(document.getElementById('bsSyncDialog').open,'Sync details popup did not open');
            document.getElementById('bsCloseSync').click();
            sync.click();sync.click();
            await new Promise(resolve=>setTimeout(resolve,20));
            check(syncPosts===1&&sync.disabled,'Duplicate sync not prevented');
            await new Promise(resolve=>setTimeout(resolve,2700));
            check(synced&&!sync.disabled,'Completed sync did not restore button');
            check(document.getElementById('bsSyncReportBody').textContent.includes('Dictionary searches'),'Missing source limitations report');
            check(document.getElementById('bsEmpty').hidden,'Indexed text still shows empty reader');
            const words=[...document.querySelectorAll('#bsVerses .bs-word')];
            check(words.length===2,'Not every indexed word is clickable');
            check(document.getElementById('bsVerses').textContent.includes('Alpha 😀 beta.'),'Codepoint offsets corrupted text');
            check(!document.getElementById('bsConcordanceHeading'),'Removed explanatory section still displayed');
            words[1].click();await new Promise(resolve=>setTimeout(resolve,20));
            check(document.getElementById('bsWordResults').textContent.includes('No verified original-language equivalent'),'Missing unavailable alignment message');
            check(!document.querySelector('.bs-source-toggle'),'Unverified dictionary results leaked into NIV click');
            words[0].click();await new Promise(resolve=>setTimeout(resolve,20));
            check(document.getElementById('bsWordResults').textContent.includes('Synthetic original'),'Verified original spelling missing');
            check([...document.querySelectorAll('.bs-source-toggle')].map(button=>button.textContent).join(',')==='Mounce,Vine','Sources merged or mislabeled');
            check(!document.getElementById('bsWordResults').textContent.includes('KJV'),'Legacy lookup still displayed');
            check(document.getElementById('bsWordResults').textContent.includes('No matching entry found in this book.'),'Missing empty source message');
            document.querySelector('.bs-show-more').click();await new Promise(resolve=>setTimeout(resolve,20));
            check(document.querySelector('.bs-show-more').textContent==='Show less','Full entry not expanded');
            check(!document.querySelector('#bsWordResults img'),'Source data executed as HTML');
            document.querySelector('.bs-source-content .bs-scripture-ref').click();await new Promise(resolve=>setTimeout(resolve,20));
            check(document.getElementById('bsVerseDialog').open,'Verse popup not opened');
            check(document.getElementById('bsVerseText').textContent==='Alpha 😀 beta.','Preview lost complete verse');
            check(document.getElementById('bsBook').value==='Genesis'&&document.getElementById('bsChapter').value==='1','Reference changed current passage');
            document.getElementById('bsCopyVerse').click();await new Promise(resolve=>setTimeout(resolve,20));
            check(copied==='Genesis 1:1 (NIV)\\nAlpha 😀 beta.','Copy Verse did not copy reference and complete text');
            document.getElementById('bsCloseVerse').click();
            document.querySelector('.bs-show-more').click();
            check(document.querySelector('.bs-source-content').hidden,'Show less did not collapse the entry');
            const toggle=document.querySelector('.bs-source-toggle');toggle.click();
            check(toggle.getAttribute('aria-expanded')==='false','Source card not collapsible');
        ''', setup)

    def test_thompson_navigation_dictionary_and_chain_clicks_are_independent(self):
        setup='''
        window.events=[];window.navVersion=0;
        window.fetch=async(url,options={})=>{
            const path=String(url);events.push({path,options});let data={ok:true};
            if(path.endsWith('/status'))data={...data,schema_ready:true,indexed_verses:31087,message:'Ready'};
            else if(path.endsWith('/state')){if(options.method==='POST')data.version=++navVersion;else data={...data,storage_available:true,state:{book:'Genesis',chapter:1,verse:1,theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};}
            else if(path.includes('/chapter?'))data.verses=[{id:7,verse:1,text:'Love.',heading:'Heading',attribution:'Source',edition_label:'NIV 2011 · Thompson',red_spans:[{start_offset:0,end_offset:5}],topics:[{id:4,topic_key:'2201',title:'Love'}],tokens:[{text:'Love',token_index:0,start_offset:0,end_offset:4}]}];
            else if(path.includes('/word/'))data={...data,word:'Love',query:'Love',verified:false,result_type:'dictionary',explanation:'Dictionary explanations, not a verified NIV word alignment.',sources:[{source:'mounce',label:'Mounce',available:true,next_offset:null,entries:[{id:3,sense_id:9,headword:'LOVE',section:'Expository dictionary',language:'Greek',preview:'A source explanation.',evidence:['Definition cites this verse'],identifiers:[]}]},{source:'vine',label:'Vine',available:true,next_offset:null,entries:[]}]};
            else if(path.includes('/dictionaries/entry/'))data.entry={source_title:'Mounce',identifiers:[],blocks:[{type:'paragraph',text:'Preserved definition.'}]};
            else if(path.includes('/chain/'))data.topic={key:'2201',title:'Love',source_page:1600,blocks:[
                {type:'paragraph',role:'intro',text:'Love'},
                {type:'heading',role:'section',text:'2. Examples'},
                {type:'paragraph',role:'references',text:'John 3:16',references:[{start:0,end:9,book:'John',chapter:3,verse:16}]},
                {type:'heading',role:'subheading',text:'Christ'},
                {type:'paragraph',role:'verse',text:'John 3:16 Complete indexed verse.',references:[{start:0,end:9,book:'John',chapter:3,verse:16}]},
                {type:'paragraph',role:'verse',text:'Another source verse.'},
                {type:'paragraph',role:'related',text:'→ Giving Freely, 2115.'}]};
            else if(path.includes('/verse?'))data.verse={reference:'John 3:16',text:'Complete indexed verse.',attribution:'NIV'};
            else if(path.endsWith('/history')){if(options.method!=='POST')data.history=[{book:'Psalms',chapter:1,verse:1,visited_at:'2026-10-21T00:00:00Z',snippet:'Blessed is the one.'}];}
            else throw Error('Unexpected URL '+path);
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''
        verify='''
            const el=name=>document.getElementById('bs'+name),wait=()=>new Promise(r=>setTimeout(r,25));
            check(document.querySelector('.bs-word.bs-red-letter'),'Verified red text not displayed');
            document.querySelector('.bs-word').click();await wait();
            check(!el('WordResults').textContent.includes('not a verified NIV'),'Hidden explanatory text still displayed');
            check(document.querySelector('.bs-source-toggle').textContent==='Mounce','Mounce missing');
            document.querySelector('.bs-show-more').click();await wait();
            check(events.some(e=>e.path.includes('/entry/3?sense=9')),'Whole article used instead of matching definition');
            const clicks=events.filter(e=>e.path.includes('/word/')).length;
            document.querySelector('.bs-chain-margin button').click();await wait();
            check(el('ChainDialog').open,'Chain popup missing');
            check(el('ChainContent').querySelectorAll('.bs-chain-verse').length===2,'Source verses merged into continuous text');
            check(el('ChainContent').querySelector('.bs-chain-section').tagName==='H5','Numbered section heading missing');
            check(el('ChainContent').querySelector('.bs-chain-subheading').textContent==='Christ','Example heading missing');
            check(!el('ChainContent').querySelector('.bs-chain-intro'),'Duplicate topic heading displayed');
            const references=el('ChainContent').querySelector('.bs-chain-references');
            check(parseFloat(getComputedStyle(references).paddingLeft)>0,'Reference list not indented');
            const paragraphs=el('ChainContent').querySelectorAll('.bs-chain-verse');
            check(paragraphs[1].getBoundingClientRect().top>=paragraphs[0].getBoundingClientRect().bottom,'Verse paragraphs overlap');
            check(el('ChainDialog').scrollWidth<=el('ChainDialog').clientWidth+1,'Chain popup overflows horizontally');
            check(events.filter(e=>e.path.includes('/word/')).length===clicks,'Chain click triggered word lookup');
            el('ChainContent').querySelector('.bs-scripture-ref').click();await wait();
            check(el('VerseDialog').open&&el('ChainDialog').open,'Verse popup replaced chain popup');
            check(el('VerseText').textContent==='Complete indexed verse.','Incomplete preview');
            check(el('Book').value==='Genesis','Popup navigated reader');el('CloseVerse').click();el('CloseChain').click();
            el('Navigate').click();check(el('NavigationDialog').open,'Book sheet missing');
            check(el('BookGrid').querySelectorAll('button').length===39,'Incomplete Old Testament book grid');
            check(el('BookGrid').querySelector('[aria-current=true]').getAttribute('aria-label')==='Genesis','Current book not highlighted');
            el('NewTestament').click();
            check(el('BookGrid').querySelectorAll('button').length===27,'Incomplete New Testament book grid');
            el('BookGrid').querySelector('[aria-label="John"]').click();
            check(!el('ChapterGrid').hidden&&el('ChapterGrid').querySelectorAll('button').length===21,'Book did not open chapters');
            el('ChapterGrid').querySelector('[aria-label="John 3"]').click();await wait();
            check(el('Book').value==='John'&&el('Chapter').value==='3'&&!el('NavigationDialog').open,'Chapter selection failed');
            el('Navigate').click();check(!el('BookGrid').hidden&&el('ChapterGrid').hidden,'Reopening navigator returned to chapters');check(el('NewTestament').getAttribute('aria-pressed')==='true','Wrong testament on reopen');check(el('BookGrid').querySelector('[aria-current=true]').getAttribute('aria-label')==='John','Current John book not highlighted');el('ReferenceInput').value='Ps 1:1';el('DirectReference').requestSubmit();await wait();
            check(el('Book').value==='Psalms','Direct-reference navigation failed');
            el('History').click();await wait();check(el('HistoryDialog').open&&el('HistoryList').textContent.includes('Blessed'),'History missing');el('CloseHistory').click();
            check(events.filter(e=>e.options.method==='POST').every(e=>e.options.headers['X-CSRF-Token']==='test-csrf'),'Account writes lack CSRF');
        '''
        for size in ((1100,850),(390,844)):
            with self.subTest(size=size):self.run_browser(self.render_preview(),verify,setup,window_size=size)

    def test_study_tabs_concordance_chain_back_and_outside_dismissal(self):
        setup='''
        window.events=[];window.pendingTopic=null;window.deferTopic=false;
        window.fetch=async(url,options={})=>{
            const path=String(url);events.push(path);let data={ok:true};
            if(path.endsWith('/status'))data={...data,schema_ready:true,indexed_verses:31087,message:'Ready'};
            else if(path.endsWith('/state'))data={...data,storage_available:false,state:{book:'Genesis',chapter:1,verse:1,theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};
            else if(path.includes('/chapter?'))data.verses=[{id:7,verse:1,text:'Love obey.',topics:[{id:4,topic_key:'1424',title:'Glorifying God'}],tokens:[{text:'Love',token_index:0,start_offset:0,end_offset:4},{text:'obey',token_index:1,start_offset:5,end_offset:9}]}];
            else if(path.includes('/word/'))data={...data,word:path.endsWith('/0')?'Love':'obey',verified:false,result_type:'dictionary',explanation:'Dictionary explanations, not a verified NIV word alignment.',sources:[{label:'Mounce',available:true,entries:[],next_offset:null},{label:'Vine',available:true,entries:[],next_offset:null}]};
            else if(path.includes('/concordance?')){const q=new URL(path,'https://example.test').searchParams.get('q');data={...data,available:true,query:q,next_offset:null,entries:q==='missing'?[]:[{headword:q.toUpperCase(),source_page:2006,related:q==='Love'?['LOVED']:[],blocks:[{type:'paragraph',text:'John 3:16 For God so l the world',references:[{start:0,end:9,book:'John',chapter:3,verse:16}]}]}]};}
            else if(path.includes('/verse?'))data.verse={reference:'John 3:16',text:'Full verse preview.',attribution:'NIV'};
            else if(path.endsWith('/chain/4'))data.topic={id:4,key:'1424',title:'Glorifying God',source_page:1649,blocks:[...Array.from({length:25},()=>({type:'paragraph',text:'A source paragraph to allow scrolling.'})),{type:'paragraph',role:'related',text:'→ Praise, 1452.',chain_links:[{start:2,end:14,topic_id:5,key:'1452'}]}]};
            else if(path.endsWith('/chain/5')){
                if(deferTopic)await new Promise(resolve=>{pendingTopic=resolve});
                data.topic={id:5,key:'1452',title:'Praise',source_page:1650,blocks:[{type:'paragraph',text:'John 3:16',references:[{start:0,end:9,book:'John',chapter:3,verse:16}]}]};
            }else throw Error('Unexpected URL '+path);
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''
        verify='''
            const el=name=>document.getElementById('bs'+name),wait=()=>new Promise(r=>setTimeout(r,25));
            const outside=dialog=>{dialog.dispatchEvent(new PointerEvent('pointerdown',{clientX:0,clientY:0,bubbles:true}));dialog.dispatchEvent(new MouseEvent('click',{clientX:0,clientY:0,bubbles:true}));};
            check(!el('CommentaryTab').disabled,'Commentary tab is unavailable');
            check(!document.querySelector('.bs-study-header .bs-kicker'),'Circled header remains');
            check(!el('WordPanel').textContent.includes('Mounce & Vine'),'Circled subtitle remains');
            check(getComputedStyle(document.querySelector('.bs-handwritten')).fontStyle==='italic','Handwritten title not italic');
            document.querySelectorAll('.bs-word')[0].click();await wait();
            check(!el('WordResults').textContent.includes('not a verified'),'Circled explanation remains');
            el('ConcordanceTab').click();await wait();
            check(el('ConcordanceTab').getAttribute('aria-selected')==='true'&&el('WordPanel').hidden,'Concordance tab did not activate');
            check(el('ConcordanceResults').textContent.includes('John 3:16 For God so l the world'),'Short source excerpt missing');
            check(el('ConcordanceSearch').value==='Love','Selected word lost when switching tabs');
            const wordRequests=events.filter(p=>p.includes('/word/')).length;
            document.querySelectorAll('.bs-word')[1].click();await wait();
            check(el('ConcordanceResults').textContent.includes('OBEY'),'Word click did not use active concordance tab');
            check(events.filter(p=>p.includes('/word/')).length===wordRequests,'Concordance click also searched dictionaries');
            el('WordTab').click();await wait();
            check(el('Search').value==='obey','Switching tabs lost word selection');
            el('WordTab').dispatchEvent(new KeyboardEvent('keydown',{key:'ArrowRight',bubbles:true}));await wait();
            check(el('ConcordanceTab').getAttribute('aria-selected')==='true','Tab keyboard navigation failed');
            el('ConcordanceSearch').value='missing';el('ConcordanceForm').requestSubmit();await wait();
            check(el('ConcordanceResults').textContent.includes('No entry'),'Missing-entry state absent');
            el('ConcordanceSearch').value='Love';el('ConcordanceForm').requestSubmit();await wait();
            el('ConcordanceResults').querySelector('.bs-concordance-form').click();await wait();
            check(el('ConcordanceSearch').value==='LOVED','Printed related-form link failed');
            el('ConcordanceResults').querySelector('.bs-scripture-ref').click();await wait();
            check(el('VerseDialog').open,'Concordance reference did not open full verse');
            el('VerseText').click();check(el('VerseDialog').open,'Click inside verse closed popup');
            outside(el('VerseDialog'));await wait();check(!el('VerseDialog').open,'Verse backdrop did not close popup');
            document.querySelector('.bs-chain-margin button').click();await wait();
            el('ChainDialog').scrollTop=200;const oldScroll=el('ChainDialog').scrollTop;
            el('ChainContent').querySelector('.bs-chain-ref').click();await wait();
            check(el('ChainTitle').textContent.includes('1452')&&!el('ChainBack').hidden,'Related chain did not open with Back');
            el('ChainContent').querySelector('.bs-scripture-ref').click();await wait();
            outside(el('VerseDialog'));await wait();
            check(el('ChainDialog').open&&!el('VerseDialog').open,'Nested dismissal closed underlying chain');
            el('ChainBack').click();await wait();
            check(el('ChainTitle').textContent.includes('1424'),'Back lost original topic');
            check(Math.abs(el('ChainDialog').scrollTop-oldScroll)<=1,'Back lost scroll position');
            check(el('ChainBack').hidden,'Back remained active with empty history');
            deferTopic=true;el('ChainContent').querySelector('.bs-chain-ref').click();await wait();
            outside(el('ChainDialog'));await wait();check(!el('ChainDialog').open,'Chain backdrop did not close popup');
            pendingTopic();await wait();check(!el('ChainDialog').open,'Late chain response reopened closed popup');
            check(el('Book').value==='Genesis','Study navigation changed Bible passage');
        '''
        for size in ((1100,900),(390,844)):
            with self.subTest(size=size):self.run_browser(self.render_preview(),verify,setup,window_size=size)

    def test_banners_person_choices_book_study_and_testament_reopening(self):
        setup='''
        window.events=[];
        const person={id:9,name:'Noah',label:'Noah — Righteous man',sections:[{title:'Bible Character Studies — Noah',source_page:1893,blocks:[{type:'paragraph',text:'Noah walked with God. Genesis 6:9.',references:[{start:22,end:33,book:'Genesis',chapter:6,verse:9}]}]},{title:'Journeys and source charts',source_page:1893,pages:[1893],blocks:[]}],topics:[]};
        window.fetch=async(url,options={})=>{
            const path=String(url);events.push(path);let data={ok:true};
            if(path.endsWith('/status'))data={...data,schema_ready:true,indexed_verses:31087,message:'Ready'};
            else if(path.endsWith('/state'))data={...data,storage_available:false,state:{book:'Genesis',chapter:1,verse:1,theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};
            else if(path.includes('/chapter?'))data.verses=Array.from({length:30},(_,i)=>({id:7+i,verse:i+1,text:'Noah Mary.',topics:[],tokens:[{text:'Noah',token_index:0,start_offset:0,end_offset:4},{text:'Mary',token_index:1,start_offset:5,end_offset:9}]}));
            else if(path.includes('/word/'))data={...data,word:path.endsWith('/0')?'Noah':'Mary',verified:false,result_type:'dictionary',sources:[],people:path.endsWith('/0')?{people:[{id:9,label:person.label}],selected:person}:{people:[{id:10,label:'Mary — Mother of Jesus'},{id:11,label:'Mary — Magdalene'}],selected:null}};
            else if(path.endsWith('/people/11'))data.person={id:11,name:'Mary',label:'Mary — Magdalene',sections:[{title:'Thompson biography',source_page:2004,blocks:[{type:'paragraph',text:'The Magdalene source entry.'}]}],topics:[]};
            else if(path.includes('/book-study?'))data={...data,available:true,study:{title:new URL(path,'https://example.test').searchParams.get('book'),source_page:22,blocks:Array.from({length:30},(_,i)=>({type:'paragraph',text:i?'Thompson outline paragraph.':'AUTHOR: Source introduction. Genesis 3:15.',references:i?[]:[{start:29,end:41,book:'Genesis',chapter:3,verse:15}]}))}};
            else if(path.includes('/verse?'))data.verse={reference:'Genesis 3:15',text:'Full source verse.',attribution:'NIV'};
            else throw Error('Unexpected URL '+path);
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''
        verify='''
            const el=name=>document.getElementById('bs'+name),wait=()=>new Promise(r=>setTimeout(r,25));
            const headings=[...document.querySelectorAll('.bs-banner h2')];
            check(headings.length===4,'Not all tool headings use banners');
            check(headings.every(h=>getComputedStyle(h).fontStyle==='italic'),'Heading typography differs');
            check(new Set(headings.map(h=>getComputedStyle(h).fontFamily)).size===1,'Heading fonts differ');
            check(getComputedStyle(el('WordPanel')).getPropertyValue('--bs-tool-tint')!==getComputedStyle(el('ConcordancePanel')).getPropertyValue('--bs-tool-tint'),'Tool banner colours identical');
            document.querySelector('#bsVerse1 .bs-word').click();await wait();
            check(el('WordResults').querySelector('.bs-person-study').textContent.includes('Noah — Righteous man'),'Person profile absent');
            check(el('WordResults').querySelectorAll('.bs-study-section').length===2,'Person source sections missing');
            const chart=el('WordResults').querySelectorAll('details')[1];chart.open=true;chart.querySelector('button').click();
            check(el('SourceDialog').open,'Source chart popup missing');el('CloseSource').click();await wait();
            document.querySelectorAll('#bsVerse1 .bs-word')[1].click();await wait();
            check(el('WordResults').querySelectorAll('.bs-person-choice').length===2,'Ambiguous person choices missing');
            check(!el('WordResults').querySelector('.bs-person-study h4'),'Ambiguous person silently selected');
            el('WordResults').querySelectorAll('.bs-person-choice')[1].click();await wait();
            check(el('WordResults').querySelector('.bs-person-study h4').textContent==='Mary — Magdalene','Wrong person selected');
            el('Bible').scrollTop=160;const before=el('Bible').scrollTop;
            el('BookStudyTab').click();await wait();
            check(el('ReaderPanel').hidden&&!el('BookStudyPanel').hidden,'Book Study tab failed');
            check(el('BookStudyTitle').textContent==='Genesis','Wrong book study');
            check(el('BookStudyResults').textContent.includes('Source introduction'),'Book source content missing');
            el('BookStudyResults').querySelector('.bs-scripture-ref').click();await wait();
            check(el('VerseDialog').open,'Book study reference preview missing');el('CloseVerse').click();await wait();
            el('Bible').scrollTop=200;el('ReaderTab').click();await wait();
            check(Math.abs(el('Bible').scrollTop-before)<2,'Bible position lost after Book Study');
            el('BookStudyTab').click();await wait();check(el('Bible').scrollTop===200,'Book Study scroll position lost');
            el('Navigate').click();check(!el('BookGrid').hidden&&el('ChapterGrid').hidden,'Navigator opened chapters');
            check(el('OldTestament').getAttribute('aria-pressed')==='true','Wrong initial testament');
            check(el('BookGrid').querySelector('[aria-current=true]').getAttribute('aria-label')==='Genesis','Current book highlight absent');
            el('NewTestament').click();check(el('BookGrid').querySelectorAll('button').length===27,'New Testament selector failed');
            el('BookGrid').querySelector('[aria-label="John"]').click();
            el('ChapterGrid').querySelector('[aria-label="John 3"]').click();await wait();
            check(el('BookStudyTitle').textContent==='John','Book Study did not follow passage navigation');
            el('Navigate').click();
            check(el('NewTestament').getAttribute('aria-pressed')==='true','Reopened testament lost');
            check(!el('BookGrid').hidden&&el('ChapterGrid').hidden,'Reopened navigator showed chapters');
            check(el('BookGrid').querySelector('[aria-current=true]').getAttribute('aria-label')==='John','Reopened current book highlight wrong');
            el('BookGrid').querySelector('[aria-label="John"]').click();
            check(el('ChapterGrid').querySelector('[aria-current=true]').getAttribute('aria-label')==='John 3','Current chapter not highlighted after book click');
            el('CloseNavigation').click();
        '''
        for size in ((1100,900),(390,844)):
            with self.subTest(size=size):self.run_browser(self.render_preview(),verify,setup,window_size=size)

    def test_commentary_passage_selection_inline_outline_and_mobile_chapter_swipes(self):
        setup='''
        window.events=[];window.pendingCommentary=null;window.pendingSeries=null;window.deferSeries=false;
        window.fetch=async(url,options={})=>{
            const path=String(url),q=new URL(path,'https://example.test').searchParams;events.push(path);let data={ok:true};
            if(path.endsWith('/status'))data={...data,schema_ready:true,indexed_verses:31087,message:'Ready'};
            else if(path.endsWith('/state'))data={...data,storage_available:false,state:{book:'Colossians',chapter:2,verse:10,theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};
            else if(path.includes('/chapter?'))data.verses=Array.from({length:30},(_,i)=>({id:i+1,verse:i+1,text:'Faith.',tokens:[{text:'Faith',token_index:0,start_offset:0,end_offset:5}]}));
            else if(path.includes('/commentary?')){
                if(q.get('verse')==='2')await new Promise(resolve=>{pendingCommentary=resolve});
                if(q.get('series')==='exalting-jesus'&&deferSeries)await new Promise(resolve=>{pendingSeries=resolve});
                data={...data,available:true,reference:q.get('book')+' '+q.get('chapter')+':'+q.get('verse'),entries:[{source_title:q.get('series')==='exalting-jesus'?'Exalting Jesus in Colossians':'Be Complete',title:'Original <source> heading',author:'Source author',passage:'Colossians 2:4–15',blocks:Array.from({length:6},(_,i)=>({type:'paragraph',text:'Exact source paragraph '+i+'.'}))}]};
                if(q.get('series')==='exalting-jesus')data.entries[0].blocks=[
                    {type:'paragraph',role:'commentary_main_idea',text:'Main Idea: The source main idea.'},
                    {type:'paragraph',role:'commentary_outline',level:1,text:'I. A source heading.'},
                    {type:'paragraph',role:'commentary_outline',level:2,text:'A. A source subheading.'},
                    ...data.entries[0].blocks,
                    {type:'heading',role:'commentary_heading',level:1,marker:'I.',text:'A source heading'},
                    {type:'heading',role:'commentary_heading',level:2,marker:'A.',text:'A source subheading'}];
            }else if(path.includes('/book-study?'))data={...data,available:true,study:{source_page:1406,blocks:[{type:'heading',text:'MAJOR CHAINS IN COLOSSIANS'},{type:'paragraph',role:'major_chain',text:'Eternal Hope · 1692',chain_links:[{start:0,end:19,key:'1692',topic_id:4}]}],outline:{blocks:[{type:'heading',text:'NEW TESTAMENT'},{type:'paragraph',text:'Exact condensed outline for this book.'}]}}};
            else if(path.endsWith('/chain/4'))data.topic={key:'1692',title:'Eternal Hope',source_page:1600,blocks:[{type:'paragraph',text:'Exact chain source.'}]};
            else throw Error('Unexpected request '+path);
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''
        verify='''
            const el=name=>document.getElementById('bs'+name),wait=()=>new Promise(r=>setTimeout(r,25));
            const mobile=matchMedia('(max-width:700px)').matches;
            check(!document.querySelector('.bs-preview,.bs-sync-actions'),'Ordinary user sees diagnostic controls');
            if(!mobile){
                check(document.querySelector('.app-main').getBoundingClientRect().width>innerWidth-40,'Desktop workspace remains narrow');
                check(el('Layout').getBoundingClientRect().height>innerHeight*.65,'Desktop reading height is underused');
            }
            el('WordTab').dispatchEvent(new KeyboardEvent('keydown',{key:'End',bubbles:true}));await wait();
            check(el('CommentaryTab').getAttribute('aria-selected')==='true'&&el('WordPanel').hidden,'Commentary tab did not activate');
            check(el('CommentaryResults').textContent.includes('Colossians 2:10'),'Commentary lost selected verse');
            check(!el('CommentaryResults').querySelector('source'),'Indexed markup executed as HTML');
            check(!el('CommentaryResults').querySelector('details,summary'),'Commentary still requires Read more');
            check(el('CommentaryResults').textContent.includes('Exact source paragraph 5.'),'Full original commentary is not immediately visible');
            document.querySelector('#bsVerse2 .bs-word').click();await wait();
            document.querySelector('#bsVerse3 .bs-word').click();await wait();
            pendingCommentary();await wait();
            check(el('CommentaryResults').querySelector('h3').textContent==='Colossians 2:3','Late commentary replaced newer passage');
            check(!events.some(p=>p.includes('/word/')),'Commentary word click also opened dictionary');
            const exalting=document.querySelector('[data-commentary-series="exalting-jesus"]'),wiersbe=document.querySelector('[data-commentary-series="wiersbe"]');
            deferSeries=true;exalting.click();await wait();wiersbe.click();await wait();pendingSeries();await wait();
            check(el('CommentaryResults').textContent.includes('Be Complete'),'Late series response replaced current commentary');
            deferSeries=false;exalting.click();await wait();
            check(exalting.getAttribute('aria-pressed')==='true'&&wiersbe.getAttribute('aria-pressed')==='false','Selected commentary highlight is wrong');
            check(el('CommentaryResults').querySelector('h3').textContent==='Colossians 2:3','Changing series changed the selected verse');
            check(el('CommentaryResults').textContent.includes('Exalting Jesus in Colossians'),'Exalting Jesus did not load');
            check(el('CommentaryResults').textContent.includes('Exact source paragraph 5.')&&!el('CommentaryResults').querySelector('details'),'Exalting Jesus is not fully visible');
            const outline=el('CommentaryResults').querySelector('.bs-commentary-outline-card');
            check(outline&&outline.querySelectorAll('.bs-commentary-outline').length===2,'Source outline was not grouped');
            const main=el('CommentaryResults').querySelector('h5.bs-commentary-heading'),sub=el('CommentaryResults').querySelector('h6.bs-commentary-heading');
            check(main.textContent.startsWith('I.')&&sub.textContent.startsWith('A.'),'Body outline markers missing');
            check(getComputedStyle(main).fontWeight==='700'&&getComputedStyle(sub).fontWeight==='700','Body outline headings are not bold');
            check(parseFloat(getComputedStyle(main).fontSize)>parseFloat(getComputedStyle(sub).fontSize),'Body heading hierarchy is not visible');
            check(parseFloat(getComputedStyle(outline.querySelector('.bs-commentary-level-2')).paddingLeft)>0,'Outline subpoints are not indented');
            el('BookStudyTab').click();await wait();
            check(el('BookStudyResults').querySelector('.bs-condensed-outline').textContent.includes('Exact condensed outline'),'Inline outline absent');
            check(!el('BookStudyResults').querySelector('.bs-source-pages'),'Outline still opens an image');
            el('BookStudyResults').querySelector('.bs-chain-ref').click();await wait();
            check(el('ChainDialog').open&&el('ChainContent').textContent.includes('Exact chain source'),'Book major chain link failed');
            el('CloseChain').click();await wait();el('ReaderTab').click();await wait();
            const swipe=(dx,dy=0)=>{
                const target=el('Scripture');
                target.dispatchEvent(new PointerEvent('pointerdown',{pointerType:'touch',pointerId:7,isPrimary:true,clientX:200,clientY:200,bubbles:true}));
                target.dispatchEvent(new PointerEvent('pointermove',{pointerType:'touch',pointerId:7,isPrimary:true,clientX:200+dx,clientY:200+dy,bubbles:true}));
                target.dispatchEvent(new PointerEvent('pointerup',{pointerType:'touch',pointerId:7,isPrimary:true,clientX:200+dx,clientY:200+dy,bubbles:true}));
            };
            swipe(-120);await wait();
            check(el('Chapter').value===(mobile?'3':'2'),'Touch navigation has wrong viewport behavior');
            if(mobile){
                swipe(120);await wait();check(el('Chapter').value==='2','Swipe right did not go back');
                swipe(-10,160);await wait();check(el('Chapter').value==='2','Vertical scrolling changed chapter');
                swipe(-25);await wait();check(el('Chapter').value==='2','Short gesture changed chapter');
                const selected=getSelection(),range=document.createRange();range.selectNodeContents(document.querySelector('#bsVerse1 .bs-word'));selected.addRange(range);
                swipe(-120);await wait();check(el('Chapter').value==='2','Selecting text changed chapter');selected.removeAllRanges();
                el('Book').value='Jude';el('Book').dispatchEvent(new Event('change'));await wait();
                swipe(-120);await wait();check(el('Book').value==='Revelation'&&el('Chapter').value==='1','Swipe did not cross book boundary');
                swipe(120);await wait();check(el('Book').value==='Jude','Reverse swipe did not cross book boundary');
                el('Book').value='Genesis';el('Book').dispatchEvent(new Event('change'));await wait();
                swipe(120);await wait();check(el('Book').value==='Genesis'&&el('Chapter').value==='1','Swipe passed first chapter');
            }
            check(exalting.getAttribute('aria-pressed')==='true','Chapter navigation lost commentary choice');
            check(events.filter(p=>p.includes('/commentary?')).at(-1).includes('series=exalting-jesus'),'Chapter navigation requested the wrong commentary');
        '''
        for size in ((1440,900),(390,844)):
            with self.subTest(size=size):self.run_browser(self.render_preview(),verify,setup,window_size=size)

    def test_bulletin_insight_is_invisible_when_no_note_matches(self):
        html='''<html><head></head><body><section id="maxwellInsight" hidden style="display:none"></section>
        <script id="maxwellInsightConfig" type="application/json">{"url":"/bulletin/leadership-insight","pending":true,"reference":"Genesis 1:1","day":"2026-10-21"}</script>
        <script src="/static/bulletin_insight.js"></script></body></html>'''
        self.run_browser(html, '''
            const card=document.getElementById('maxwellInsight');
            check(card.hidden&&getComputedStyle(card).display==='none','Missing commentary displays an empty card');
            check(!card.textContent,'Missing commentary displays a placeholder');
        ''', '''window.fetch=async()=>new Response(JSON.stringify({ok:true,insight:null,pending:false,reference:'Genesis 1:1',day:'2026-10-21'}),{headers:{'Content-Type':'application/json'}});''')

    def test_bulletin_insight_renders_safe_expandable_summary(self):
        html='''<html><head></head><body><section id="maxwellInsight" hidden style="display:none"></section>
        <script id="maxwellInsightConfig" type="application/json">{"url":"/bulletin/leadership-insight","pending":true,"reference":"Genesis 1:1","day":"2026-10-21"}</script>
        <script src="/static/bulletin_insight.js"></script></body></html>'''
        self.run_browser(html, '''
            const card=document.getElementById('maxwellInsight');
            check(!card.hidden&&getComputedStyle(card).display!=='none','Matching insight remains hidden');
            check(!card.textContent.includes('AI-generated summary'),'Removed summary label remains');
            check(card.textContent.includes('Maxwell Leadership Bible'),'Source attribution missing');
            check(card.querySelector('.maxwell-source-content').textContent==='Exact indexed source.\\nSecond original paragraph.','Read more did not preserve indexed text');
            check(!card.querySelector('img'),'AI text executed as HTML');
            card.querySelector('summary').click();await new Promise(resolve=>setTimeout(resolve,20));
            check(card.querySelector('summary').textContent==='Read less','Expanded article control failed');
        ''', '''window.fetch=async()=>new Response(JSON.stringify({ok:true,reference:'Genesis 1:1',day:'2026-10-21',insight:{title:'<img src=x onerror=alert(1)>',passage:'Genesis 1:1-2',summary:'Synthetic supported summary.',content:'Exact indexed source.\\nSecond original paragraph.'}}),{headers:{'Content-Type':'application/json'}});''')


if __name__ == '__main__':
    unittest.main()
