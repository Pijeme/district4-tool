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
            DictLoader({'base.html': '<html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head><body class="district-ui district-glass bible-study-page"><header class="app-header"><div class="header-left"><button id="outsideBible" class="menu-toggle">☰</button><div class="logo-wrap"></div><div class="header-text"><div class="app-title">District 4 Tool <span class="app-section-title">Bible Study</span></div></div></div></header><main class="app-main">{% block content %}{% endblock %}</main></body></html>'}),
            FileSystemLoader(str(ROOT / 'templates')),
        ])
        with app.test_request_context():
            html = app.jinja_env.get_template('bible_study.html').render(
                csrf_token='test-csrf', bible_study_admin=admin,
                bible_books=[{'name': name, 'chapters': count} for name, count in bible_study.BOOKS])
        baseline = '<style>' + '\n'.join((ROOT/'static'/name).read_text(encoding='utf-8') for name in
                                       ('styles.css','visual_upgrade.css','glass_theme.css')) + '</style>'
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
            check(!root.querySelector('.bs-personal-tools'),'Disabled annotation placeholder still occupies reading space');
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
            check(divider.getAttribute('aria-valuenow')==='52','Mobile divider does not resize vertically');
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

    def personal_setup(self):
        return '''
        window.savedAnnotations=[];window.savedBookmarks=[];window.savedPosts=[];window.lookupCount=0;window.copied='';window.prefVersion=0;
        Object.defineProperty(navigator,'clipboard',{value:{writeText:async text=>{copied=text}},configurable:true});
        window.fetch=async(url,options={})=>{
            const path=String(url);let data={ok:true};
            if(path.endsWith('/status'))data={...data,schema_ready:true,indexed_verses:31087,message:'Ready'};
            else if(path.endsWith('/state'))data=options.method==='POST'?{...data,version:++prefVersion}:{...data,storage_available:true,state:{book:'Genesis',chapter:1,verse:1,theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};
            else if(path.includes('/chapter?'))data.verses=[{id:7,verse:1,text:'Alpha 😀 beta.',heading:'Fixture heading',edition_label:'NIV 2011 · Thompson',topics:[{id:3,topic_key:'123',title:'Topic'}],tokens:[{text:'Alpha',token_index:0,start_offset:0,end_offset:5},{text:'beta',token_index:1,start_offset:8,end_offset:12}]}];
            else if(path.includes('/word/')){lookupCount++;data={...data,word:'Alpha',verified:false,sources:[]};}
            else if(path.includes('/personal?'))data={...data,annotations:savedAnnotations,bookmarks:savedBookmarks};
            else if(path.endsWith('/annotations')){
                const body=JSON.parse(options.body);savedPosts.push({body,options});
                const item={...body,id:'saved-annotation',version:1,reference:'Genesis 1:1',verse:1,verse_id:7,delete_url:'/pastor-resources/bible-study/api/personal/annotations/saved-annotation'};
                savedAnnotations.push(item);data.annotation=item;
            }else if(path.endsWith('/bookmarks')){
                const body=JSON.parse(options.body);savedPosts.push({body,options});
                const item={...body,id:'saved-bookmark',reference:'Genesis 1:1',verse:1,delete_url:'/pastor-resources/bible-study/api/personal/bookmarks/saved-bookmark'};
                savedBookmarks=[item];data.bookmark=item;
            }else if(path.includes('/personal/')){
                if(options.method==='DELETE'){
                    if(path.includes('/annotations/'))savedAnnotations=[];else savedBookmarks=[];
                }else if(options.method==='PATCH'){savedAnnotations[0].note=JSON.parse(options.body).note;savedAnnotations[0].version++;}
                else data.item={available:true,book:'Genesis',chapter:1,verse:1};
            }else if(path.endsWith('/history'))data.history=[];
            else if(path.includes('/book-study?'))data={...data,available:false};
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''

    def test_global_library_handles_bible_ids_and_protected_deletion_alongside_ebooks(self):
        app = Flask('saved_library_browser')
        app.add_url_rule('/pastor-resources', endpoint='pastor_resources', view_func=lambda: '')
        app.jinja_loader = DictLoader({'base.html': '<html><head></head><body>{% block content %}{% endblock %}</body></html>'})
        with app.test_request_context():
            html = app.jinja_env.from_string(resources.PASTOR_MY_LIBRARY_HTML).render(initial_tab='highlights', is_admin=False)
        self.run_browser(html, '''
            check(document.querySelector('#annotationsList a').getAttribute('href').includes('annotation=abc'),'Global note lost Bible jump link');
            check(!document.querySelector('#annotationsList img'),'Saved note executed as HTML');
            document.querySelector('#annotationsList [data-saved-id="bible:abc"]').click();
            await new Promise(resolve=>setTimeout(resolve,20));
            check(deletions[0].url.endsWith('/personal/annotations/abc'),'Bible deletion went to ebook API');
            check(deletions[0].options.headers['X-CSRF-Token']==='global-token','Global Bible delete lost CSRF');
            document.querySelector('#bookmarksList [data-saved-id="12"]').click();
            await new Promise(resolve=>setTimeout(resolve,20));
            check(deletions[1].url==='/pastor-resources/api/bookmarks/12','Ebook bookmark delete regressed');
        ''', '''
            window.deletions=[];window.confirm=()=>true;
            window.fetch=async(url,options={})=>{
                if(options.method==='DELETE'){deletions.push({url:String(url),options});return new Response(JSON.stringify({ok:true}));}
                return new Response(JSON.stringify({ok:true,bible_csrf_token:'global-token',annotations:[{id:'bible:abc',source:'bible',format:'BIBLE',book_title:'Bible · Genesis 1:1',selected_text:'Alpha',note:'<img src=x onerror=alert(1)>',jump_url:'/pastor-resources/bible-study?annotation=abc',delete_url:'/pastor-resources/bible-study/api/personal/annotations/abc'}],bookmarks:[{id:12,book_title:'Ebook',jump_url:'/read/1?bookmark=12'}]}));
            };
        ''')

    def test_text_selection_does_not_lookup_words_and_saves_precise_unicode_marks_and_notes(self):
        self.run_browser(self.render_preview(), '''
            const wait=()=>new Promise(resolve=>setTimeout(resolve,150));
            const text=document.querySelector('.bs-verse-text'),words=text.querySelectorAll('.bs-word');
            words[0].click();await wait();check(lookupCount===1,'Word tap no longer opens study');
            const range=document.createRange();range.setStart(text.childNodes[1],1);range.setEnd(words[1].firstChild,4);
            const selection=getSelection();selection.removeAllRanges();selection.addRange(range);document.dispatchEvent(new Event('selectionchange'));await wait();
            check(!document.getElementById('bsSelectionToolbar').hidden,'Text selection has no action toolbar');
            words[1].click();check(lookupCount===1,'Selecting text opened word lookup');
            document.querySelector('[data-selection-action="highlight"][data-color="#9ee6b8"]').click();await wait();
            check(savedPosts[0].options.headers['X-CSRF-Token']==='test-csrf','Annotation save lacks CSRF');
            const span=savedPosts[0].body.ranges[0];
            check(span.start_offset===6&&span.end_offset===12&&span.selected_text==='😀 beta','UTF-16 offsets corrupted Bible anchor');
            check(text.textContent==='Alpha 😀 beta.','Highlight changed verse text');
            check(text.querySelectorAll('mark').length>0,'Saved highlight not painted');
            check(document.querySelector('.bs-verse-mark'),'No marker opens saved annotation');
            document.querySelector('.bs-verse-number').click();await wait();
            check(!document.getElementById('bsSelectionToolbar').hidden,'Verse action menu vanished after selection cleared');
            document.querySelector('[data-selection-action="copy"]').click();await wait();
            check(copied==='Genesis 1:1 (NIV)\\nAlpha 😀 beta.','Copy lost text or reference');
            document.querySelector('.bs-verse-number').click();document.querySelector('[data-selection-action="note"]').click();
            document.getElementById('bsNoteText').value='<img src=x onerror=alert(1)> My note';
            document.getElementById('bsNoteForm').dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));await wait();
            check(savedAnnotations.length===2&&savedAnnotations[1].kind==='note','Selected verse note was not saved');
            document.querySelector('.bs-verse-mark').click();check(document.getElementById('bsSavedDialog').open,'Saved marker does not open notes');
            check(!document.querySelector('#bsSavedItems img'),'Note content rendered as HTML');
            [...document.querySelectorAll('#bsSavedItems button')].find(b=>b.textContent==='Delete').click();await wait();
            check(!text.querySelector('mark'),'Deleting a highlight left its paint behind');
            Object.defineProperty(navigator,'clipboard',{value:undefined,configurable:true});
            document.execCommand=()=>false;
            document.getElementById('bsCloseSaved').click();document.querySelector('.bs-verse-number').click();
            document.querySelector('[data-selection-action="copy"]').click();await wait();
            check(document.getElementById('bsCopyDialog').open&&document.getElementById('bsCopyText').value.includes('Alpha 😀 beta.'),'Local Wi-Fi copy has no usable fallback');
        ''', self.personal_setup())

    def test_mobile_settings_tabs_panel_space_and_bookmark_are_available_in_all_layouts(self):
        self.run_browser(self.render_preview(admin=False), '''
            const wait=()=>new Promise(resolve=>setTimeout(resolve,150));
            const root=document.getElementById('bibleStudyRoot'),bible=document.getElementById('bsBible'),study=document.getElementById('bsStudy');
            check(root.getBoundingClientRect().bottom<=innerHeight+1,'Mobile reader extends beyond screen');
            check(bible.clientHeight>200&&study.clientHeight>200,'Reader controls leave too little panel space');
            check(document.getElementById('bsBookStudyTab').textContent==='Book Study','Book Study tab was lost');
            check(!document.getElementById('bsSync'),'Ordinary reader can see developer sync');
            document.getElementById('bsSettings').click();check(document.getElementById('bsSettingsDialog').open,'Gear did not open settings');
            document.getElementById('bsShowMargins').checked=false;document.getElementById('bsShowMargins').dispatchEvent(new Event('change'));
            check(getComputedStyle(document.querySelector('.bs-chain-margin')).display==='none','Margin preference failed');
            check(document.querySelector('#bsSettingsDialog a[href$="tab=highlights"]'),'Global notes link missing');
            document.querySelector('[data-bs-mode="study"]').click();await wait();
            check(getComputedStyle(bible).display==='none','Study fullscreen failed');
            document.getElementById('bsSettings').click();check(document.getElementById('bsSettingsDialog').open,'Gear inaccessible in study fullscreen');
            document.querySelector('[data-bs-mode="split"]').click();await wait();
            document.querySelector('.bs-verse-number').click();document.querySelector('[data-selection-action="bookmark"]').click();await wait();
            check(savedBookmarks.length===1&&savedBookmarks[0].verse_id===7,'Verse bookmark was not saved');
            document.getElementById('bsBookStudyTab').click();await wait();check(!document.getElementById('bsBookStudyPanel').hidden,'Book Study no longer opens');
        ''', self.personal_setup(), window_size=(420,900))

    def test_short_slower_diagonal_touch_swipes_survive_pointer_cancel_and_preserve_selection(self):
        self.run_browser(self.render_preview(), '''
            const area=document.getElementById('bsScripture'),word=area.querySelector('.bs-word');
            const touch=(x,y)=>new Touch({identifier:1,target:word,clientX:x,clientY:y});
            const send=(type,x,y)=>word.dispatchEvent(new TouchEvent(type,{bubbles:true,cancelable:true,touches:type==='touchend'?[]:[touch(x,y)],changedTouches:[touch(x,y)]}));
            send('touchstart',180,200);send('touchmove',165,205);
            area.dispatchEvent(new PointerEvent('pointercancel',{pointerId:1,bubbles:true}));
            await new Promise(resolve=>setTimeout(resolve,550));
            send('touchend',125,226);await new Promise(resolve=>setTimeout(resolve,40));
            check(document.getElementById('bsChapter').value==='2','Short, slower diagonal swipe did not navigate');
            const currentWord=area.querySelector('.bs-word');
            const t=(x,y)=>new Touch({identifier:2,target:currentWord,clientX:x,clientY:y});
            currentWord.dispatchEvent(new TouchEvent('touchstart',{bubbles:true,touches:[t(100,200)],changedTouches:[t(100,200)]}));
            currentWord.dispatchEvent(new TouchEvent('touchmove',{bubbles:true,cancelable:true,touches:[t(105,280)],changedTouches:[t(105,280)]}));
            currentWord.dispatchEvent(new TouchEvent('touchend',{bubbles:true,touches:[],changedTouches:[t(105,280)]}));
            check(document.getElementById('bsChapter').value==='2','Vertical reading gesture changed chapters');
            const range=document.createRange();range.selectNodeContents(currentWord);getSelection().removeAllRanges();getSelection().addRange(range);
            send('touchstart',180,200);send('touchmove',120,200);send('touchend',110,200);
            check(document.getElementById('bsChapter').value==='2','Text selection allowed a chapter swipe');
        ''',self.personal_setup(),window_size=(390,844))

    def test_pulpit_sections_greek_popup_and_selection_work_on_desktop_and_mobile(self):
        setup=self.personal_setup().replace('window.fetch=async(url,options={})=>{','''
        window.commentaryQueries=[];window.dictionaryQueries=[];
        window.greekAnchor={key:'greek-source',category:'commentary',title:'The Pulpit Commentary',label:'Commentary',text:'Exhorted (παρεκάλεσα). An original explanation to entreat or beseech.',token:'signed-source-token'};
        window.fetch=async(url,options={})=>{''').replace("if(path.endsWith('/history'))data.history=[];",'''
            if(path.includes('/commentary?')){
                commentaryQueries.push(path);
                const hom=path.includes('section=homiletics');
                data={...data,available:true,reference:'1 Timothy 1:3',study_anchors:[greekAnchor],entries:[{source_title:'The Pulpit Commentary',author:'Printed author',title:hom?'The heretic.':'Exposition',passage:'1 Timothy 1:3',blocks:hom?[{type:'paragraph',text:'Original homiletics text.'}]:[
                    {type:'paragraph',text:greekAnchor.text,_study_texts:{text:greekAnchor},runs:[{text:'Exhorted',marks:['strong']},{text:greekAnchor.text.slice(8),marks:[]}],greek_links:[{start:10,end:20}],greek_source:{book_id:54,chapter:1,verse:3,source_title:'The Pulpit Commentary',passage:'1 Timothy 1:3',source_section:1145}},
                    {type:'paragraph',text:'A continuing source explanation with a passage John 3:16.',references:[{start:47,end:56,book:'John',chapter:3,verse:16}]}]}]};
            }else if(path.includes('/pulpit-greek?')){
                check(path.includes('book=1+Timothy'),'Greek lookup lost the source passage');data.match={lemma:'παρακαλέω',query:'G3870'};
            }else if(path.includes('/dictionaries/search?')){dictionaryQueries.push(path);data={...data,query:'G3870',sources:[]};}
            else if(path.endsWith('/history'))data.history=[];''')
        verify='''
            const wait=()=>new Promise(resolve=>setTimeout(resolve,150));
            document.getElementById('bsCommentaryTab').click();await wait();
            document.querySelector('[data-commentary-series="pulpit"]').click();await wait();
            check(!document.getElementById('bsPulpitSections').hidden,'Pulpit section choices hidden');
            const greek=document.querySelector('#bsCommentaryResults .bs-greek-ref');
            check(greek.textContent==='παρεκάλεσα','Greek source offsets changed');
            greek.click();await wait();
            check(document.getElementById('bsGreekDialog').open,'Greek tap did not open source explanation');
            check(document.querySelector('#bsGreekContent strong').textContent==='Exhorted','Original emphasis lost');
            const source=document.querySelector('#bsGreekContent [data-study-source]');
            check(source&&source.textContent===greekAnchor.text,'Greek popup lost the immutable source anchor');
            const range=document.createRange();range.selectNodeContents(source);getSelection().removeAllRanges();getSelection().addRange(range);
            document.dispatchEvent(new Event('selectionchange'));await wait();
            check(!document.getElementById('bsSelectionToolbar').hidden,'Greek explanation cannot use reading actions');
            getSelection().removeAllRanges();document.getElementById('bsCloseGreek').click();
            await new Promise(resolve=>setTimeout(resolve,700));greek.click();await wait();
            check(document.getElementById('bsGreekDialog').open,'Greek popup did not reopen after selection');
            check(document.querySelector('#bsGreekActions button'),'Reopened Greek popup lost its dictionary action');
            document.querySelector('#bsGreekActions button').click();await wait();
            check(document.getElementById('bsWordTab').getAttribute('aria-selected')==='true','Open Word Study did not switch tabs');
            check(dictionaryQueries.at(-1).includes('q=G3870'),'Dictionary lookup did not use the matched identifier');
            document.getElementById('bsCommentaryTab').click();await wait();
            document.querySelector('[data-pulpit-section="homiletics"]').click();await wait();
            check(document.getElementById('bsCommentaryResults').textContent.includes('Original homiletics text.'),'Homiletics did not display full text');
            check(commentaryQueries.at(-1).includes('section=homiletics'),'Pulpit section choice not sent');
            document.querySelector('[data-commentary-series="moody"]').click();await wait();
            check(document.getElementById('bsPulpitSections').hidden,'Pulpit controls leaked into another series');
        '''
        for size in ((1440,900),(390,844)):
            with self.subTest(size=size):self.run_browser(self.render_preview(),verify,setup,window_size=size)

    def test_rapid_tab_switches_share_pending_requests_and_keep_loaded_commentary_dom(self):
        setup=self.personal_setup().replace('window.fetch=async(url,options={})=>{','''
        window.commentaryFetches=0;window.releaseCommentary=null;
        window.fetch=async(url,options={})=>{''').replace("if(path.endsWith('/history'))data.history=[];",'''
            if(path.includes('/commentary?')){
                commentaryFetches++;
                await new Promise(resolve=>{releaseCommentary=resolve});
                data={...data,available:true,reference:'Genesis 1:1',entries:[{title:'Original passage',source_title:'Original source',author:'Source author',passage:'Genesis 1:1',blocks:[{type:'paragraph',text:'A complete original paragraph that remains visible when switching tabs.'}]}]};
            }else if(path.endsWith('/history'))data.history=[];''')
        self.run_browser(self.render_preview(),'''
            const wait=()=>new Promise(resolve=>setTimeout(resolve,100));
            const commentary=document.getElementById('bsCommentaryTab'),word=document.getElementById('bsWordTab');
            commentary.click();await wait();word.click();commentary.click();await wait();
            check(commentaryFetches===1,'Switching back duplicated an in-flight commentary request');
            releaseCommentary();await wait();
            const article=document.querySelector('#bsCommentaryResults article');
            check(article&&article.textContent.includes('complete original paragraph'),'Shared response did not reach the active tab');
            for(let i=0;i<20;i++){word.click();commentary.click();}
            await wait();
            check(commentaryFetches===1,'Loaded commentary was downloaded again on each tab switch');
            check(document.querySelector('#bsCommentaryResults article')===article,'Loaded text was needlessly rebuilt');
        ''',setup,window_size=(390,844))

    def test_sermon_titles_follow_chapter_only_and_source_selections_save_and_reopen(self):
        setup=self.personal_setup().replace('window.fetch=async(url,options={})=>{','''
        window.studyRecords=[];window.sermonQueries=[];
        window.outlineBlocks=[
            {type:'heading',role:'sermon_heading',text:'I. A SOURCE HEADING'},
            {type:'paragraph',role:'sermon_subpoint',text:'1. An italic subpoint.',runs:[{text:'1. An italic subpoint.',marks:['em']}]},
            {type:'paragraph',text:'A printed bullet.',list_kind:'bullet',list_level:1,list_item:10},
            {type:'paragraph',text:'A nested supporting bullet.',list_kind:'bullet',list_level:2,list_item:11},
            {type:'paragraph',text:'Third ordered point.',list_kind:'ordered',list_level:1,list_item:12,list_number:3,list_style:'I'},
            {type:'paragraph',text:'Nested evidence.',list_kind:'bullet',list_level:2,list_item:13},
            {type:'paragraph',text:'Continuation of the third point.',list_kind:'ordered',list_level:1,list_item:12,list_number:3,list_style:'I'},
            {type:'paragraph',text:'Seventh ordered point.',list_kind:'ordered',list_level:1,list_item:14,list_number:7,list_style:'I'}];
        window.studyAnchor={key:'sermon-source-key',category:'sermon',title:'Fixture Collection',label:'Sermon Outlines',origin:'/pastor-resources/bible-study/api/sermons/fixture',path:['sermon','blocks',0,'text'],text:'An original sermon paragraph with a source citation John 3:16.',token:'fixture-signed-token'};
        window.fetch=async(url,options={})=>{''')
        setup=setup.replace("else if(path.includes('/personal?'))data={...data,annotations:savedAnnotations,bookmarks:savedBookmarks};", "else if(path.includes('/personal?'))data={...data,annotations:savedAnnotations,bookmarks:savedBookmarks,study_annotations:studyRecords.filter(i=>i.kind!=='bookmark'),study_bookmarks:studyRecords.filter(i=>i.kind==='bookmark')};")
        setup=setup.replace("else if(path.endsWith('/history'))data.history=[];",'''else if(path.includes('/sermons?')){sermonQueries.push(path);data.sermons=[{id:'fixture',title:'A chapter sermon'},{id:'last',title:'Nicoll entry'}];}
            else if(path.includes('/sermons/'))data={...data,study_anchors:[studyAnchor],sermon:{id:'fixture',title:'A chapter sermon',author:'Printed Author',source_title:'Fixture Collection',publisher:'Fixture Publisher',purpose:'Funeral message',source_section:4,ranges:[{book_id:1,chapter:1,verse:1,end_chapter:1,end_verse:23}],blocks:[{type:'paragraph',text:studyAnchor.text,_study_texts:{text:studyAnchor},references:[{start:51,end:60,book:'John',chapter:3,verse:16}]},...outlineBlocks]}};
            else if(path.endsWith('/study-saves')){
                const body=JSON.parse(options.body);savedPosts.push({body,options});
                const item={...body,id:'study-save',format:'STUDY',reference:'Sermon Outlines',book_author:'Fixture Collection',version:1,delete_url:'/pastor-resources/bible-study/api/study-saves/study-save',ranges:body.ranges.map(span=>({...span,source_key:studyAnchor.key,source:studyAnchor,block_text:span.text}))};studyRecords.push(item);data.item=item;
            }else if(path.endsWith('/history'))data.history=[];''')
        self.run_browser(self.render_preview(),'''
            const wait=()=>new Promise(resolve=>setTimeout(resolve,150));
            document.getElementById('bsSermonTab').click();await wait();
            check(sermonQueries.length===1&&sermonQueries[0].includes('book=Genesis')&&sermonQueries[0].includes('chapter=1')&&!sermonQueries[0].includes('verse='),'Sermon suggestions do not use chapter-only filtering');
            check(document.getElementById('bsSermonResults').textContent==='A chapter sermonNicoll entry','Results are not titles only or ordering changed');
            document.querySelector('.bs-word').click();await wait();
            check(sermonQueries.length===1&&lookupCount===0,'Word selection refreshed sermon suggestions by verse');
            document.querySelector('.bs-sermon-title').click();await wait();
            check(document.getElementById('bsSermonMetadata').textContent.includes('Printed Author')&&document.getElementById('bsSermonMetadata').textContent.includes('Funeral message'),'Author/occasion missing inside opened sermon');
            const content=document.getElementById('bsSermonContent');
            check(content.querySelector('h5.bs-sermon-heading'),'Outline Roman heading is not a heading');
            check(content.querySelector('.bs-sermon-subpoint em'),'Subpoint emphasis lost');
            check(content.querySelector('ul ul li').textContent==='A nested supporting bullet.','Nested source bullets flattened');
            const ordered=content.querySelector('ol');
            check(ordered.start===3&&ordered.type==='I'&&ordered.children.length===2&&ordered.lastElementChild.value===7,'Original list numbering or item continuation lost');
            check(ordered.firstElementChild.textContent.includes('Continuation of the third point.'),'List item continuation moved outside its item');
            check(content.scrollWidth<=content.clientWidth+1,'Mobile outline overflows its panel');
            const source=document.querySelector('#bsSermonContent [data-study-source]');check(source,'Original sermon has no selectable source anchor');
            const range=document.createRange();range.selectNodeContents(source);getSelection().removeAllRanges();getSelection().addRange(range);document.dispatchEvent(new Event('selectionchange'));await wait();
            check(!document.getElementById('bsSelectionToolbar').hidden,'Sermon text selection has no actions');
            document.querySelector('[data-selection-action="highlight"][data-color="#9ed3ff"]').click();await wait();
            check(studyRecords.length===1&&savedPosts[0].body.ranges[0].token==='fixture-signed-token','Study highlight did not save its signed source');
            check(source.querySelector('mark'),'Study highlight not rendered');
            document.querySelector('.bs-study-mark').click();await wait();
            [...document.querySelectorAll('#bsSavedItems button')].find(b=>b.textContent==='Open source').click();await wait();
            check(document.getElementById('bsSavedStudyDialog').open&&document.getElementById('bsSavedStudyContent').textContent.includes('An original sermon'),'Saved study did not return to its indexed source');
            check(document.querySelector('#bsSavedStudyContent ul ul li'),'Global source jump flattened the outline');
            document.getElementById('bsCloseSavedStudy').click();document.getElementById('bsSermonBack').click();
            document.getElementById('bsNext').click();await wait();check(sermonQueries.length===2&&sermonQueries[1].includes('chapter=2'),'Chapter navigation did not refresh sermons');
        ''',setup,window_size=(390,844))

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
            check(sync.closest('#bsSettingsDialog'),'Sync is not inside reading settings');
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
            check(headings.length===5,'Not all tool headings use banners');
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
            const touch=mobile||navigator.maxTouchPoints>0;
            check(!document.querySelector('.bs-preview,.bs-sync-actions'),'Ordinary user sees diagnostic controls');
            if(!mobile){
                check(document.querySelector('.app-main').getBoundingClientRect().width>innerWidth-40,'Desktop workspace remains narrow');
                check(el('Layout').getBoundingClientRect().height>innerHeight*.65,'Desktop reading height is underused');
            }
            el('CommentaryTab').click();await wait();
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
            check(el('Chapter').value===(touch?'3':'2'),'Touch navigation has wrong device behavior');
            if(touch){
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
        for size in ((1440,900),(390,844),(1024,900)):
            tablet="Object.defineProperty(navigator,'maxTouchPoints',{value:5});" if size[0]==1024 else ''
            with self.subTest(size=size):self.run_browser(self.render_preview(),verify,tablet+setup,window_size=size)

    def test_study_loading_prioritizes_bible_and_visible_tab_then_only_selected_background_sources(self):
        setup='''
        window.events=[];window.releaseBible=null;window.releaseCommentary=null;window.releaseTitles=null;
        window.fetch=async(url)=>{
            const path=String(url),q=new URL(path,'https://example.test').searchParams;events.push(path);let data={ok:true};
            if(path.endsWith('/status'))data={...data,schema_ready:true,indexed_verses:31087,message:'Ready'};
            else if(path.endsWith('/state'))data={...data,storage_available:false,state:{book:'Colossians',chapter:2,verse:1,theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};
            else if(path.includes('/chapter?')){
                await new Promise(resolve=>{releaseBible=resolve});
                data.verses=[{id:1,verse:1,text:'Faith.',tokens:[{text:'Faith',token_index:0,start_offset:0,end_offset:5}]}];
            }else if(path.includes('/commentary?')){
                if(q.get('chapter')==='2')await new Promise(resolve=>{releaseCommentary=resolve});
                data={...data,available:true,reference:'Source passage',entries:[{source_title:'Moody',title:'Original heading',author:'Source editor',passage:'Colossians 2',blocks:[{type:'paragraph',text:'Original paragraph.'}]}]};
            }else if(path.includes('/sermons?')){
                if(q.get('chapter')==='3')await new Promise(resolve=>{releaseTitles=resolve});
                data.sermons=[{id:'one',title:'A source theme'}];
            }else throw Error('Unexpected request '+path);
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''
        verify='''
            const el=name=>document.getElementById('bs'+name),wait=(ms=25)=>new Promise(r=>setTimeout(r,ms));
            el('CommentaryTab').click();document.querySelector('[data-commentary-series="moody"]').click();await wait();
            check(!events.some(x=>x.includes('/commentary?')||x.includes('/sermons?')),'Study loaded before Bible');
            releaseBible();await wait();
            check(events.filter(x=>x.includes('/commentary?')).length===1,'Foreground commentary requested twice');
            check(events.find(x=>x.includes('/commentary?')).includes('series=moody'),'Wrong selected provider loaded');
            await wait(700);check(!events.some(x=>x.includes('/sermons?')),'Hidden tab loaded before visible commentary finished');
            releaseCommentary();await wait(700);
            check(events.filter(x=>x.includes('/sermons?')).length===1,'Sermon titles did not warm after commentary');
            el('SermonTab').click();await wait();
            check(el('SermonResults').textContent.includes('A source theme'),'Prefetched titles were not rendered');
            check(events.filter(x=>x.includes('/sermons?')).length===1,'Prefetched titles fetched again');
            el('Next').click();await wait();
            check(!events.some(x=>x.includes('/sermons?')&&x.includes('chapter=3')),'New titles preceded Bible');
            releaseBible();await wait();
            check(events.at(-1).includes('/sermons?')&&events.at(-1).includes('chapter=3'),'Visible sermon tab did not get priority');
            await wait(700);check(!events.some(x=>x.includes('/commentary?')&&x.includes('chapter=3')),'Background commentary preceded visible titles');
            releaseTitles();await wait(700);
            check(events.some(x=>x.includes('/commentary?')&&x.includes('chapter=3')),'Selected commentary did not warm after titles');
            check(events.filter(x=>x.includes('/commentary?')).every(x=>x.includes('series=moody')),'Other providers loaded eagerly');
            check(!events.some(x=>x.includes('/sermons/')),'Full sermons loaded in the background');
        '''
        self.run_browser(self.render_preview(),verify,setup,window_size=(1100,900))

    def test_tab_dimensions_stay_fixed_when_reading_panels_resize(self):
        setup=self.personal_setup()
        verify='''
            const root=document.getElementById('bibleStudyRoot'),divider=document.getElementById('bsDivider');
            const dimensions=()=>[...root.querySelectorAll('.bs-study-tabs button')].map(x=>[x.getBoundingClientRect().width,x.getBoundingClientRect().height]);
            const positions=()=>[...root.querySelectorAll('.bs-study-tabs button')].map(x=>[x.getBoundingClientRect().x,x.getBoundingClientRect().y]);
            const oldPositions=positions();
            const before=dimensions(),reader=root.querySelector('.bs-reader'),study=document.getElementById('bsStudy');
            const old=study.getBoundingClientRect();
            divider.dispatchEvent(new KeyboardEvent('keydown',{key:'Home',bubbles:true}));
            await new Promise(r=>setTimeout(r,25));
            const after=dimensions(),changed=study.getBoundingClientRect();
            check(JSON.stringify(before)===JSON.stringify(after),'Tab sizes changed with divider');
            if(!matchMedia('(max-width:700px)').matches)check(JSON.stringify(oldPositions)===JSON.stringify(positions()),'Wide tab positions followed the divider');
            check(Math.abs(old.width-changed.width)>5||Math.abs(old.height-changed.height)>5,'Reading panel failed to resize');
            const tabs=document.getElementById('bsSermonTab').parentElement;tabs.scrollLeft=tabs.scrollWidth;
            const last=tabs.querySelector('button:last-child').getBoundingClientRect(),row=tabs.getBoundingClientRect();
            check(last.right<=row.right+1,'Last study tab cannot be reached');
        '''
        for size in ((1440,900),(390,844),(820,1000)):
            with self.subTest(size=size):self.run_browser(self.render_preview(),verify,setup,window_size=size)

    def test_webkit_first_load_restored_passage_and_tablet_tabs_remain_visible(self):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.skipTest('Optional Playwright WebKit is required')
        source=(ROOT/'static/bible_study.js').read_text(encoding='utf-8')
        html=re.sub(r'<script src="[^"]*bible_study\.js[^\"]*"[^>]*></script>',
                    lambda _: '<script>'+source.replace('</script',r'<\/script')+'</script>',self.render_preview())
        setup=self.personal_setup()+'''
        const fixtureFetch=window.fetch;
        window.fetch=async(url,options)=>{
            const response=await fixtureFetch(url,options),data=await response.json();
            if(String(url).endsWith('/state')&&!options?.method)data.state.verse=17;
            if(String(url).includes('/chapter?'))data.verses=Array.from({length:30},(_,i)=>({...data.verses[0],id:i+7,verse:i+1}));
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };
        '''
        html=html.replace('<head>','<head><script>'+setup+'</script>')
        with sync_playwright() as p:
            try:browser=p.webkit.launch(headless=True)
            except Exception as error:self.skipTest('WebKit executable unavailable: '+str(error).splitlines()[0])
            try:
                for width,height in ((393,852),(820,1000),(1024,768)):
                    with self.subTest(width=width):
                        context=browser.new_context(viewport=dict(width=width,height=height),is_mobile=True,has_touch=True)
                        page=context.new_page();page.set_content(html);page.wait_for_selector('#bsVerse17')
                        page.wait_for_timeout(80)
                        def assert_navigation():
                            self.assertTrue(page.evaluate('''()=>{const n=document.querySelector('#bsNavigate'),r=n.getBoundingClientRect(),h=document.querySelector('.app-header').getBoundingClientRect();return r.top>=h.bottom&&r.bottom<=innerHeight&&n.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));}'''))
                            self.assertEqual(page.evaluate('scrollY'),0)
                        assert_navigation()
                        self.assertGreater(page.locator('#bsBible').evaluate('e=>e.scrollTop'),0,'Fixture did not restore a scrolled passage')
                        if width>700:
                            def tab_positions():return page.locator('.bs-study-tabs button').evaluate_all('(items)=>items.map(e=>{const r=e.getBoundingClientRect();return [r.x,r.y,r.width,r.height]})')
                            before=tab_positions()
                            for key in ('Home','End'):
                                page.locator('#bsDivider').dispatch_event('keydown',dict(key=key));page.wait_for_timeout(20)
                                self.assertEqual(tab_positions(),before)
                                self.assertGreaterEqual(page.locator('#bsBible').bounding_box()['width'],219)
                                self.assertTrue(page.evaluate('''()=>[...document.querySelectorAll('#bsWideTabs button')].every(e=>{const r=e.getBoundingClientRect();return r.left>=0&&r.right<=innerWidth;})'''))
                        page.evaluate('window.scrollTo(0,100);window.dispatchEvent(new Event("pageshow"))')
                        assert_navigation()
                        page.set_viewport_size(dict(width=width,height=height-150));page.wait_for_timeout(40)
                        assert_navigation()
                        page.locator('#bsNavigate').click();self.assertTrue(page.locator('#bsNavigationDialog').evaluate('e=>e.open'))
                        page.locator('#bsCloseNavigation').click()
                        # Crossing the phone breakpoint moves each unique tab
                        # group back into its own pane and restores it on return.
                        if width>700:
                            page.set_viewport_size(dict(width=393,height=852));page.wait_for_timeout(40)
                            self.assertEqual(page.locator('#bsBible>.bs-reader-tabs').count(),1)
                            self.assertEqual(page.locator('#bsStudy>.bs-study-tabs').count(),1)
                            self.assertEqual(page.locator('#bsSermonTab').count(),1)
                            page.set_viewport_size(dict(width=width,height=height));page.wait_for_timeout(40)
                            self.assertEqual(page.locator('#bsWideTabs [role=tab]').count(),6)
                            assert_navigation()
                        context.close()
            finally:browser.close()

    def test_rapid_chapter_navigation_skips_obsolete_background_work(self):
        setup='''
        window.requests=[];window.bibles=[];
        window.fetch=async(url)=>{
            const path=String(url),q=new URL(path,'https://example.test').searchParams;requests.push(path);let data={ok:true};
            if(path.endsWith('/status'))data={...data,schema_ready:true,indexed_verses:31087,message:'Ready'};
            else if(path.endsWith('/state'))data={...data,storage_available:false,state:{book:'Genesis',chapter:1,verse:1,theme:'paper',font_size:19,layout_mode:'split',split_ratio:60,version:0}};
            else if(path.includes('/chapter?')){await new Promise(resolve=>bibles.push(resolve));data.verses=[];}
            else if(path.includes('/commentary?'))data={...data,available:true,entries:[]};
            else if(path.includes('/sermons?'))data.sermons=[];
            return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}});
        };'''
        verify='''
            const wait=ms=>new Promise(r=>setTimeout(r,ms));
            document.getElementById('bsNext').click();document.getElementById('bsNext').click();await wait(25);
            check(bibles.length===3,'Rapid chapters did not request the new Bible text');
            bibles[2]();await wait(700);bibles[0]();bibles[1]();await wait(50);
            const background=requests.filter(x=>x.includes('/commentary?')||x.includes('/sermons?'));
            check(background.length===2&&background.every(x=>x.includes('chapter=3')),'Obsolete chapters were warmed');
            check(document.getElementById('bsChapter').value==='3','Old Bible response replaced current chapter');
        '''
        self.run_browser(self.render_preview(),verify,setup)

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
            check(card.querySelector('h3').textContent==='Daily Devotional','Devotional title not updated');
            check(card.querySelector('.maxwell-source-content').textContent==='Exact indexed source.\\nSecond original paragraph.','Read more did not preserve indexed text');
            check(!card.querySelector('img'),'AI text executed as HTML');
            card.querySelector('summary').click();await new Promise(resolve=>setTimeout(resolve,20));
            check(card.querySelector('summary').textContent==='Read less','Expanded article control failed');
        ''', '''window.fetch=async()=>new Response(JSON.stringify({ok:true,reference:'Genesis 1:1',day:'2026-10-21',insight:{title:'<img src=x onerror=alert(1)>',passage:'Genesis 1:1-2',excerpt:'Exact indexed source.',content:'Exact indexed source.\\nSecond original paragraph.'}}),{headers:{'Content-Type':'application/json'}});''')


if __name__ == '__main__':
    unittest.main()
