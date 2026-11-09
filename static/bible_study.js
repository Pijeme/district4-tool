/* SQLite reader and developer sync controls. Source markup is never rendered. */
(function () {
    "use strict";
    const root = document.getElementById("bibleStudyRoot");
    if (!root) return;
    const config = JSON.parse(document.getElementById("bsConfig").textContent);
    const element = name => document.getElementById("bs" + name);
    const books = [...element("Book").options].map(option => ({name: option.value, chapters: Number(option.dataset.chapters)}));
    const mobile = window.matchMedia("(max-width: 700px)");
    const touchNavigation = () => navigator.maxTouchPoints > 0 || window.matchMedia("(any-pointer: coarse)").matches || mobile.matches;
    const state = {translation: "NIV", book: "Genesis", chapter: 1, verse: 1, theme: "paper", font_size: 19, layout_mode: "split", split_ratio: 60, version: 0};
    let storageAvailable = false, saveChain = Promise.resolve(), writePaused = false, previousFocus = null;
    let contentAvailable = false, chapterRequest = 0, wordRequest = 0, verseRequest = 0;
    let lastVerseReference = null,notesVerseTarget=null;
    let copiedVerse = "", syncTimer = null, syncBusy = false;
    let navigationBook = state.book, chainRequest = 0;
    let studyTab = "word", selectedWord = null, concordanceRequest = 0, currentChain = null;
    let dictionaryPreviewRequest=0,currentDictionaryPreview=null;
    const dictionaryHistory=[];
    let navigationTestament = "old", readerTab = "bible", bookStudyRequest = 0, bookStudyBook = "";
    const readerScroll = {bible: 0, book: 0, notes: 0};
    let readerPassageChanged = false;
    let commentaryRequest = 0, commentarySeries = "wiersbe", pulpitSection = "exposition", greekRequest = 0;
    let commentaryTheme="",commentaryThemeOwner="",commentaryScrollToTheme="";
    let commentarySections=[],commentarySelectedSection="",commentarySectionsOwner="";
    let commentaryVerseTarget=null,commentaryVerseParts=[];
    let illustrationMode="suggested",illustrationRequest=0,illustrationPassage=null,illustrationSuggestionKey="";
    let illustrationLetter="A",illustrationTopic="",illustrationQuery="",illustrationSource="",illustrationArticle=null,illustrationListScroll=0;
    const commentaryNames = {wiersbe: "Wiersbe · Be Series", "exalting-jesus": "Exalting Jesus", moody: "The Moody Bible Commentary", pulpit: "The Pulpit Commentary"};
    const iosViewport=/iPad|iPhone|iPod/.test(navigator.userAgent)||(navigator.platform==="MacIntel"&&navigator.maxTouchPoints>1);
    root.dataset.iosViewport=String(iosViewport);
    function sizeWorkspace() {
        if(document.body.classList.contains("bible-study-page")) {
            // Size the app shell, rather than measuring a root whose position
            // can change during Safari's initial scroll restoration or zoom.
            const viewport=window.visualViewport;
            if(!viewport || Math.abs(viewport.scale-1)<.01) {
                document.body.style.setProperty("--bs-viewport-height",Math.round(viewport?.height || window.innerHeight)+"px");
                if(iosViewport){
                    const active=document.activeElement,editing=Boolean(active?.matches("input:not([type=button]):not([type=checkbox]),textarea,[contenteditable=true]"));
                    const keyboard=editing&&window.innerHeight-(viewport?.height||window.innerHeight)>120;
                    document.body.classList.toggle("bs-ios-keyboard",keyboard);
                    document.body.style.setProperty("--bs-viewport-offset-top",keyboard?Math.max(0,viewport?.offsetTop||0)+"px":"0px");
                }
            }
            return;
        }
        const top = root.getBoundingClientRect().top;
        root.style.setProperty("--bs-page-top", Math.ceil(top) + "px");
        root.style.setProperty("--bs-workspace-height", Math.max(140, (window.visualViewport?.height || window.innerHeight) - top) + "px");
    }
    window.addEventListener("resize", sizeWorkspace);
    window.visualViewport?.addEventListener("resize", sizeWorkspace);
    if(iosViewport){
        window.visualViewport?.addEventListener("scroll",sizeWorkspace);
        document.addEventListener("focusin",sizeWorkspace);
        document.addEventListener("focusout",()=>{requestAnimationFrame(sizeWorkspace);setTimeout(sizeWorkspace,350);});
    }
    if (document.querySelector(".app-header")) new ResizeObserver(sizeWorkspace).observe(document.querySelector(".app-header"));
    window.addEventListener("pageshow",sizeWorkspace);
    sizeWorkspace();
    const readerTabs=root.querySelector(".bs-reader-tabs"),studyTabs=element("Study").querySelector(".bs-study-tabs");
    // The desktop/tablet control row is independent of the draggable panes.
    const notesDock=element("NotesTools").closest(".bs-notes-dock"),notesDockHome=element("NotesPanel");
    const fixedTools=node("div",undefined,"bs-fixed-workspace-tools");fixedTools.id="bsFixedTools";
    const fixedReader=node("div",undefined,"bs-fixed-reader-tools"),fixedStudy=node("div",undefined,"bs-fixed-study-heading");
    fixedTools.append(fixedReader,fixedStudy);fixedTools.hidden=true;root.insertBefore(fixedTools,element("Layout"));
    const headingHomes=["Word","Concordance","Commentary","Sermon","Illustrations"].map(prefix=>({
        kind:prefix.toLowerCase(),home:element(prefix+"Panel"),heading:element(prefix+"Panel").querySelector(":scope > .bs-study-header")
    }));
    function positionWorkspaceTools(){
        const pinned=!mobile.matches&&readerTab==="notes"&&root.dataset.mode!=="study";
        fixedTools.hidden=!pinned;root.dataset.fixedNotesTools=String(pinned);
        fixedReader.dataset.hasDocument=String(!element("NotesWorkspace").hidden);
        notesDock.style.setProperty("--tool-y","28.571429%");
        if(pinned){if(notesDock.parentElement!==fixedReader)fixedReader.append(notesDock);}
        else if(notesDock.parentElement!==notesDockHome)notesDockHome.prepend(notesDock);
        fixedStudy.hidden=root.dataset.mode==="bible";
        for(const item of headingHomes){
            if(pinned&&!fixedStudy.hidden&&item.kind===studyTab){
                item.heading.style.setProperty("--tool-y",getComputedStyle(item.home).getPropertyValue("--tool-y")||"0%");
                item.heading.style.setProperty("--bs-tool-tint",getComputedStyle(item.home).getPropertyValue("--bs-tool-tint")||"#c4a052");
                if(item.heading.parentElement!==fixedStudy)fixedStudy.append(item.heading);
            }else if(item.heading.parentElement!==item.home)item.home.prepend(item.heading);
        }
    }
    function positionTabs() {
        if(mobile.matches) {
            element("WideTabs").append(readerTabs,element("LayoutControl"));element("Study").prepend(studyTabs);
        } else {
            element("WideTabs").append(readerTabs,element("LayoutControl"),studyTabs);
        }
        root.style.setProperty("--bs-reader-tab-height",(element("Bible").contains(readerTabs)?readerTabs.offsetHeight:0)+"px");
        root.style.setProperty("--bs-study-tab-height",(mobile.matches?studyTabs.offsetHeight:0)+"px");
        positionWorkspaceTools();
    }
    positionTabs();
    const readerTabsHeight = () => element("Bible").contains(readerTabs)?readerTabs.getBoundingClientRect().height:0;
    const readerChromeHeight = () => readerTabsHeight()+(readerTab==="bible"?element("BibleNavigation").getBoundingClientRect().height:0);
    const readerTabSize = new ResizeObserver(() => root.style.setProperty("--bs-reader-tab-height", readerTabsHeight() + "px"));
    readerTabSize.observe(root.querySelector(".bs-reader-tabs"));
    new ResizeObserver(()=>root.style.setProperty("--bs-study-tab-height",(mobile.matches?studyTabs.getBoundingClientRect().height:0)+"px")).observe(studyTabs);
    const chainHistory = [];
    let scrollTimer = null, scrollRestoreUntil = 0;
    const apiBase = config.statusUrl.replace(/\/status$/, "");
    const verseData = new Map();
    const studyAnchors = new Map(), boundStudyAnchors = new WeakMap();
    const sourceResponses=new Map(), sourcePending=new Map(), sourceCacheLifetime=120000;
    let commentaryRenderedKey="", commentaryRenderedAt=0;
    let chapterLoadPromise=Promise.resolve(), backgroundTimer=null, backgroundEpoch=0, backgroundBusy=false, visibleStudyLoads=0;
    let preferredStudy="commentary";

    function cancelBackgroundStudy(){++backgroundEpoch;clearTimeout(backgroundTimer);}
    function commentaryFullChapter(){return commentarySeries==="moody"||(commentarySeries==="pulpit"&&pulpitSection==="exposition");}
    function commentaryQuery(){
        const owner=[state.book,state.chapter,commentarySeries,pulpitSection].join("|");
        if(owner!==commentaryThemeOwner){clearCommentaryVerseFocus();commentaryVerseParts=[];commentaryThemeOwner=owner;commentaryTheme="";commentaryScrollToTheme="";}
        const query=new URLSearchParams({book:state.book,chapter:state.chapter,verse:commentaryFullChapter()?1:state.verse,series:commentarySeries,section:pulpitSection,scope:"chapter"});
        if(commentaryTheme&&!commentaryFullChapter())query.set("entry",commentaryTheme);
        return query;
    }
    function commentaryUrl(){return apiBase+"/commentary?"+commentaryQuery();}
    function sermonListUrl(){return apiBase+"/sermons?"+new URLSearchParams({book:state.book,chapter:state.chapter});}
    function scheduleBackgroundStudy(){
        cancelBackgroundStudy();const epoch=backgroundEpoch;
        // Expensive catalogues are on demand by default. Only explicit preview
        // configurations may opt into prefetching; ordinary Bible reading must
        // not start hidden commentary or sermon work.
        if(config.prefetchStudy!==true)return;
        if(!contentAvailable)return;
        backgroundTimer=setTimeout(()=>warmStudy(epoch),650);
    }
    async function warmStudy(epoch){
        await chapterLoadPromise;
        if(epoch!==backgroundEpoch || !contentAvailable)return;
        if(backgroundBusy || visibleStudyLoads){backgroundTimer=setTimeout(()=>warmStudy(epoch),150);return;}
        const order=studyTab==="commentary"?["sermon"]:studyTab==="sermon"?["commentary"]:
            preferredStudy==="sermon"?["sermon","commentary"]:["commentary","sermon"];
        backgroundBusy=true;
        try{
            for(const kind of order){
                if(epoch!==backgroundEpoch || visibleStudyLoads)return;
                const url=kind==="commentary"?commentaryUrl():sermonListUrl(), cached=sourceResponses.get(url);
                if(cached && Date.now()-cached.at<sourceCacheLifetime)continue;
                try{await api(url);}catch(_){/* An unavailable background source does not interrupt reading. */}
            }
        }finally{backgroundBusy=false;}
    }
    function beginStudyLoad(){
        ++visibleStudyLoads;cancelBackgroundStudy();
        return ()=>{--visibleStudyLoads;scheduleBackgroundStudy();};
    }
    function visibleStudy(action){
        const finish=beginStudyLoad();
        try{return Promise.resolve(action()).finally(finish);}catch(error){finish();return Promise.reject(error);}
    }
    let studyPersonal = {annotations: [], bookmarks: []}, sermonRequest = 0, sermonChapter = "", hydrationQueued = false;
    let personal = {annotations: [], bookmarks: []}, pendingSelection = null, editingNote = null;
    let selectionBusy = false, selectionInteractionUntil = 0, toastTimer, savedVerseFilter = null;
    const preferenceKey = "bible-reading-" + (config.preferenceKey || "preview");
    let localSettings = {};
    try { localSettings = JSON.parse(localStorage.getItem(preferenceKey) || "{}"); } catch (_error) {}
    function saveLocalSettings() { try { localStorage.setItem(preferenceKey, JSON.stringify(localSettings)); } catch (_error) {} }

    function node(tag, text, className) {
        const item = document.createElement(tag);
        if (text !== undefined) item.textContent = text;
        if (className) item.className = className;
        return item;
    }

    function loadingNotice(title, description) {
        const notice = node("div", undefined, "bs-loading-notice");
        notice.setAttribute("role", "status");
        const bar = node("progress", undefined, "bs-loading-progress");
        bar.setAttribute("aria-label", title);
        notice.append(node("h3", title), node("p", description), bar);
        return notice;
    }
    function readerNotice(title, description, busy = false) {
        element("Empty").hidden = false;
        element("LoadingTitle").textContent = title;
        element("LoadingDescription").textContent = description;
        element("LoadingProgress").hidden = !busy;
        element("Scripture").setAttribute("aria-busy", String(busy));
    }

    async function api(url, options = {}) {
        const key=String(url), cacheable=(!options.method || options.method==="GET") &&
            /\/(?:commentary\?|sermons(?:\?|\/)|book-study\?|illustrations\/(?:suggested|browse)\?|word\/|concordance\?|dictionaries\/(?:entry\/|search\?))/.test(key);
        const cached=cacheable?sourceResponses.get(key):null;
        let data;
        if(cached && Date.now()-cached.at<sourceCacheLifetime)data=cached.data;
        else {
            let pending=cacheable?sourcePending.get(key):null;
            if(!pending) {
                pending=(async()=>{
                    const response = await fetch(url, {credentials: "same-origin", cache: "no-store", ...options});
                    if (!(response.headers.get("Content-Type") || "").includes("application/json")) throw Error("Please sign in again and reopen Bible Study.");
                    const result=await response.json();
                    if(!response.ok || !result.ok){
                        const error=Error(result.error || "Bible Study is temporarily unavailable.");
                        error.status=response.status;throw error;
                    }
                    const validSource=key.includes('/commentary?')?Array.isArray(result.entries):key.includes('/sermons?')?Array.isArray(result.sermons):true;
                    if(cacheable && validSource){sourceResponses.delete(key);sourceResponses.set(key,{at:Date.now(),data:result});while(sourceResponses.size>24)sourceResponses.delete(sourceResponses.keys().next().value);}
                    return result;
                })();
                if(cacheable)sourcePending.set(key,pending);
            }
            try{data=await pending;}
            finally{if(cacheable && sourcePending.get(key)===pending)sourcePending.delete(key);}
        }
        for (const anchor of data.study_anchors || []) studyAnchors.set(anchor.category + "|" + anchor.text, anchor);
        if (studyAnchors.size > 12000) studyAnchors.clear();
        queueStudyHydration();
        return data;
    }

    function bindStudyText(container, anchor) {
        if (!anchor?.token || container.textContent !== anchor.text || container.querySelector("img")) return false;
        container.dataset.studySource = anchor.key; boundStudyAnchors.set(container, anchor);
        return true;
    }
    function queueStudyHydration() {
        if (hydrationQueued) return;
        hydrationQueued = true;
        queueMicrotask(() => {
            hydrationQueued = false;
            for (const container of root.querySelectorAll("[data-study-kind] p,[data-study-kind] h3,[data-study-kind] h4,[data-study-kind] h5,[data-study-kind] h6,[data-study-kind] li,[data-study-kind] strong")) {
                if (container.closest("[data-study-source]") || container.querySelector("[data-study-source]")) continue;
                const category = container.closest("[data-study-kind]").dataset.studyKind;
                const anchor = studyAnchors.get(category + "|" + container.textContent);
                if (bindStudyText(container, anchor)) paintStudyContainer(container);
            }
            for(const image of root.querySelectorAll(".bs-chain-chart")) {
                if(image._studyImageButton||!image.src.includes("/studies/page/"))continue;
                const button=node("button","Open chart / notes","bs-image-open");button.type="button";
                button.addEventListener("click",()=>{element("SourceImage").src=image.src;element("SourceTitle").textContent=image.alt||"Thompson study page";element("SourceDialog").showModal();});
                image.before(button);image._studyImageButton=button;
            }
        });
    }
    new MutationObserver(queueStudyHydration).observe(root, {childList:true,subtree:true});

    function paintStudyContainer(container) {
        const anchor = boundStudyAnchors.get(container);
        if (!anchor) return;
        container.querySelectorAll("mark[data-study-mark]").forEach(mark => mark.replaceWith(...mark.childNodes)); container.normalize();
        const spans = studyPersonal.annotations.flatMap(item => (item.ranges || []).filter(span => span.source_key === anchor.key).map(span => ({...span,item})));
        container._studyMarker?.remove();
        if (spans.length || studyPersonal.bookmarks.some(item=>item.ranges.some(span=>span.source_key===anchor.key))) {
            const marker=node("button","✎","bs-study-mark");marker.type="button";marker.setAttribute("aria-label","Open saved notes and marks for this source");
            marker.addEventListener("click",()=>openSavedItems());container.after(marker);container._studyMarker=marker;
        }
        const walker = document.createTreeWalker(container,NodeFilter.SHOW_TEXT), texts=[];
        let text; while ((text=walker.nextNode())) texts.push(text);
        let offset=0;
        for (const textNode of texts) {
            const chars=Array.from(textNode.textContent),start=offset,end=offset+chars.length;offset=end;
            const hits=spans.filter(span=>span.start_offset<end&&span.end_offset>start);
            if (!hits.length) continue;
            const bounds=[...new Set([start,end,...hits.flatMap(span=>[Math.max(start,span.start_offset),Math.min(end,span.end_offset)])])].sort((a,b)=>a-b);
            const fragment=document.createDocumentFragment();
            for(let i=0;i<bounds.length-1;i++) {
                const from=bounds[i],to=bounds[i+1],value=chars.slice(from-start,to-start).join("");
                const active=hits.filter(span=>span.start_offset<to&&span.end_offset>from);
                if(!active.length){fragment.append(document.createTextNode(value));continue;}
                const mark=node("mark",value);mark.dataset.studyMark=active[0].item.id;
                const highlight=active.find(span=>span.item.kind==="highlight");
                mark.style.backgroundColor=highlight?.item.color||(highlight?"#ffe66d":"transparent");
                if(active.some(span=>span.item.kind==="underline"))mark.classList.add("bs-underline");
                mark.title=active.map(span=>span.item.note).filter(Boolean).join("\n");fragment.append(mark);
            }
            textNode.replaceWith(fragment);
        }
    }
    function paintStudyMarks() { root.querySelectorAll("[data-study-source]").forEach(paintStudyContainer); }

    const translationNames = {NIV: "New International Version", CEBOCB: "Ang Pulong sa Dios", TCB: "Ang Salita ng Diyos"};
    const translationLabel = () => state.translation === "NIV" ? "NIV" : translationNames[state.translation] || "NIV";
    async function refreshTranslations() {
        try {
            const data = await api(apiBase + "/translations");
            for (const option of element("Translation").options) {
                const item = data.translations.find(item => item.code === option.value);
                option.disabled = !item?.available;
                option.title = item?.label || option.textContent;
            }
        } catch (_) { /* The selected edition reports its own unavailable content. */ }
    }
    function applyTranslation() {
        if (!translationNames[state.translation]) state.translation = "NIV";
        element("Translation").value = state.translation;
        element("EditionLabel").textContent = translationLabel();
        element("BibleKicker").textContent = "The Holy Bible · " + translationNames[state.translation];
        element("WordTab").disabled = false;
        element("WordTab").title = state.translation === "NIV" ? "" : "Study this translation with its own dictionary, then choose an English equivalent.";
        element("Search").placeholder=state.translation === "NIV" ? "Word, transliteration, H5800 or GK 6440" : state.translation === "TCB" ? "Search a Tagalog dictionary word" : "Search a Cebuano dictionary word";
    }
    element("Translation").addEventListener("change", () => {
        state.translation = element("Translation").value;
        ++wordRequest; ++concordanceRequest; ++verseRequest; selectedWord = null;
        applyTranslation(); updateChapter(); savePreferences();
        if (element("VerseDialog").open && lastVerseReference) previewVerse(lastVerseReference,notesVerseTarget);
    });

    async function loadChapter() {
        const requestId = ++chapterRequest;
        element("ReaderError").hidden = true;
        ++wordRequest;
        ++concordanceRequest; selectedWord = null;
        clearSelectionActions(); verseData.clear(); personal = {annotations: [], bookmarks: []};
        element("Verses").replaceChildren();
        element("WordResults").replaceChildren();
        element("ConcordanceResults").replaceChildren(node("p", "Select a Bible word or search the concordance."));
        readerNotice("Preparing Bible…", "Opening " + state.book + " " + state.chapter + " for a moment in the Word.", true);
        if (!contentAvailable || !config.chapterUrl) {
            readerNotice("Bible content is unavailable", "Reopen Bible Study to check library availability.");
            return;
        }
        try {
            const query = new URLSearchParams({book: state.book, chapter: state.chapter, translation: state.translation});
            const data = await api(config.chapterUrl + "?" + query);
            if (requestId !== chapterRequest) return;
            const verses = data.verses || [];
            if (verses.length) {
                element("EditionLabel").textContent = verses[0].edition_label || "NIV";
                element("BibleAttribution").textContent = verses[0].attribution || "";
            }
            element("Empty").hidden = verses.length > 0;
            element("Scripture").setAttribute("aria-busy", "false");
            if (!verses.length) readerNotice("This passage is unavailable", "Try another chapter or Bible translation.");
            let lastHeading = "";
            for (const verse of verses) {
                verseData.set(verse.id, verse);
                if (verse.heading && verse.heading !== lastHeading) {
                    element("Verses").append(node("h3", verse.heading)); lastHeading = verse.heading;
                }
                const row = node("div", undefined, (verse.topics || []).length ? "bs-verse-row" : "");
                const verseBody=node("div",undefined,"bs-verse-body");row.append(verseBody);
                const paragraph = node("p", undefined, "bs-verse" + (verse.layout === "poetry" || ["Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Songs"].includes(state.book) ? " bs-poetry" : ""));
                paragraph.id = "bsVerse" + verse.verse;
                paragraph.dataset.verseId = verse.id;
                const number = node("button", verse.verse_end > verse.verse ? verse.verse + "–" + verse.verse_end : verse.verse, "bs-verse-number"); number.type = "button";
                number.setAttribute("aria-label", "Actions for " + state.book + " " + state.chapter + ":" + verse.verse);
                number.addEventListener("click", () => {
                    selectWholeVerse(verse);
                    if(studyTab==="commentary"&&commentaryFullChapter()){
                        state.verse=verse.verse;savePreferences();requestCommentaryVerse(verse.verse);loadCommentary();
                    }
                }); paragraph.append(number);
                const textContainer = node("span", undefined, "bs-verse-text"); paragraph.append(textContainer);
                const characters = Array.from(verse.text);
                let offset = 0;
                for (const token of verse.tokens) {
                    appendScriptureText(textContainer, characters, offset, token.start_offset, verse.red_spans || []);
                    const button = node("span", token.text, "bs-word");
                    button.dataset.tokenIndex=token.token_index;
                    button.setAttribute("role", "button"); button.tabIndex = 0;
                    if ((verse.red_spans || []).some(span => token.start_offset < span.end_offset && token.end_offset > span.start_offset)) button.classList.add("bs-red-letter");
                    button.addEventListener("click", () => {
                        if (window.getSelection()?.toString() || performance.now() < suppressWordClickUntil) return;
                        clearSelectionActions();
                        root.querySelectorAll(".bs-word.is-selected").forEach(item => item.classList.remove("is-selected"));
                        button.classList.add("is-selected"); state.verse = verse.verse;
                        setIllustrationPassage(verse.verse,verse.verse_end || verse.verse);
                        savePreferences();
                        selectedWord = {word: token.text, verseId: verse.id, tokenIndex: token.token_index};
                        ++wordRequest; ++concordanceRequest;
                        element("Search").value = element("ConcordanceSearch").value = token.text;
                        if (studyTab === "concordance") loadConcordance(token.text);
                        else if (studyTab === "commentary") {requestCommentaryVerse(verse.verse);loadCommentary();}
                        else if (studyTab === "word") loadWord(verse.id, token.token_index);
                        else scheduleBackgroundStudy();
                        if (mobile.matches) requestAnimationFrame(() => {
                            const pane = element("Bible"), bounds = paragraph.getBoundingClientRect();
                            const bottom = pane.getBoundingClientRect().bottom - 12;
                            if (bounds.height < pane.clientHeight - readerChromeHeight() - 24 && bounds.bottom > bottom) {
                                pane.scrollTop += bounds.bottom - bottom;
                            }
                        });
                    });
                    button.addEventListener("keydown", event => {
                        if (event.key === "Enter" || event.key === " ") { event.preventDefault(); button.click(); }
                    });
                    textContainer.append(button); offset = token.end_offset;
                }
                appendScriptureText(textContainer, characters, offset, characters.length, verse.red_spans || []);
                verseBody.append(paragraph);
                if (verse.footnotes?.length) {
                    const notes = node("details", undefined, "bs-verse-footnotes");
                    notes.append(node("summary", "Notes · " + verse.verse));
                    for (const note of verse.footnotes) notes.append(node("p", note.text));
                    verseBody.append(notes);
                }
                if ((verse.topics || []).length) {
                    const margin = node("aside", undefined, "bs-chain-margin"); margin.setAttribute("aria-label", "Chain topics for verse " + verse.verse);
                    let extra = null;
                    verse.topics.forEach((topic, index) => {
                        const button = node("button"); button.type = "button";
                        button.title = topic.topic_key + " · " + topic.title;
                        button.append(node("span", topic.topic_key, "bs-chain-key"), node("span", topic.title.split(/\s+\(M\./)[0], "bs-chain-name"));
                        button.addEventListener("click", () => previewChain(topic.id));
                        if (index < 2) margin.append(button);
                        else {
                            if (!extra) { extra = node("details"); extra.append(node("summary", "+ " + (verse.topics.length - 2) + " topics")); margin.append(extra); }
                            extra.append(button);
                        }
                    });
                    row.append(margin);
                }
                element("Verses").append(row);
            }
            loadPersonal(requestId);
            if (verses.length && storageAvailable) {
                const target = verses.find(verse => verse.verse <= state.verse && (verse.verse_end || verse.verse) >= state.verse) || verses[0];
                api(apiBase + "/history", {method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": config.csrfToken}, body: JSON.stringify({verse_id: target.id})}).catch(() => {});
                scrollRestoreUntil = performance.now() + 500;
                if (readerTab !== "bible") return;
                if (state.verse > 1) {
                    const targetElement = element("Verse" + target.verse);
                    if (targetElement) element("Bible").scrollTop += targetElement.getBoundingClientRect().top - element("Bible").getBoundingClientRect().top - readerChromeHeight() - 20;
                } else element("Bible").scrollTop = 0;
            }
        } catch (error) {
            if (requestId === chapterRequest) {
                readerNotice("Could not prepare this passage", "Try opening this chapter again.");
                element("Status").textContent = error.message;
                element("ReaderError").textContent = error.message;
                element("ReaderError").hidden = false;
            }
        }
    }

    function appendScriptureText(parent, characters, start, end, spans) {
        const boundaries = [...new Set([start, end, ...spans.flatMap(span => [span.start_offset, span.end_offset]).filter(offset => start < offset && offset < end)])].sort((a, b) => a - b);
        for (let index = 0; index + 1 < boundaries.length; index++) {
            const from = boundaries[index], to = boundaries[index + 1], text = characters.slice(from, to).join("");
            const red = spans.some(span => from < span.end_offset && to > span.start_offset);
            parent.append(red ? node("span", text, "bs-red-letter") : document.createTextNode(text));
        }
    }

    function toast(message, duration = 4000) {
        ([...root.querySelectorAll("dialog[open]")].at(-1)||root).append(element("Toast"));
        clearTimeout(toastTimer); element("Toast").textContent = message; element("Toast").hidden = false;
        element("SaveStatus").textContent = message;
        toastTimer = setTimeout(() => { element("Toast").hidden = true; }, duration);
    }
    function jsonWrite(method, body) {
        return {method, headers: {"Content-Type": "application/json", "X-CSRF-Token": config.csrfToken}, body: JSON.stringify(body)};
    }
    function clearSelectionActions() {
        pendingSelection = null; element("SelectionToolbar").hidden = true;
    }
    function placeSelectionToolbar() {
        const dialog=[...root.querySelectorAll("dialog[open]")].at(-1);
        (dialog||root).append(element("SelectionToolbar"));
    }
    function selectionReference(ranges) {
        const first = verseData.get(ranges[0]?.verse_id), last = verseData.get(ranges.at(-1)?.verse_id);
        const end = last?.verse_end || last?.verse;
        return first ? state.book + " " + state.chapter + ":" + first.verse + (end > first.verse ? "–" + end : "") : "";
    }
    function showSelectionActions(ranges) {
        pendingSelection = {ranges, reference: selectionReference(ranges)};
        const selected=ranges.map(range=>verseData.get(range.verse_id)).filter(Boolean);
        if(selected.length)setIllustrationPassage(Math.min(...selected.map(v=>v.verse)),Math.max(...selected.map(v=>v.verse_end || v.verse)));
        element("SelectionReference").textContent = pendingSelection.reference;
        placeSelectionToolbar();
        element("SelectionToolbar").hidden = false;
    }
    function selectWholeVerse(verse) {
        window.getSelection()?.removeAllRanges();
        showSelectionActions([{verse_id: verse.id, start_offset: 0, end_offset: Array.from(verse.text).length, selected_text: verse.text}]);
        pendingSelection.wholeVerse = true;
    }
    function captureSelection() {
        if (selectionBusy || [...root.querySelectorAll("dialog[open]")].some(dialog=>!["bsChainDialog","bsVerseDialog","bsSavedStudyDialog","bsGreekDialog","bsDictionaryDialog"].includes(dialog.id))) return;
        const selected = window.getSelection();
        if (!selected?.rangeCount || selected.isCollapsed) {
            if (pendingSelection?.wholeVerse) return;
            if (performance.now() >= selectionInteractionUntil && !element("SelectionToolbar").contains(document.activeElement)) clearSelectionActions();
            return;
        }
        const range = selected.getRangeAt(0);
        if (!element("Verses").contains(range.startContainer) || !element("Verses").contains(range.endContainer)) {
            const parent = node => node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
            if (parent(range.startContainer)?.closest("[data-study-source]") && parent(range.endContainer)?.closest("[data-study-source]")) {
                const spans = [];
                for (const container of root.querySelectorAll("[data-study-source]")) {
                    if (!container.getClientRects().length || !range.intersectsNode(container)) continue;
                    const anchor=boundStudyAnchors.get(container); if(!anchor)continue;
                    const offsetAt=(boundary,offset,fallback)=>{
                        if(!container.contains(boundary))return fallback;
                        const prefix=document.createRange();prefix.selectNodeContents(container);prefix.setEnd(boundary,offset);
                        return Array.from(prefix.toString()).length;
                    };
                    const start=offsetAt(range.startContainer,range.startOffset,0),end=offsetAt(range.endContainer,range.endOffset,Array.from(anchor.text).length);
                    if(end>start)spans.push({token:anchor.token,text:anchor.text,start_offset:start,end_offset:end,selected_text:Array.from(anchor.text).slice(start,end).join("")});
                }
                if(spans.length){
                    const first=boundStudyAnchors.get(parent(range.startContainer).closest("[data-study-source]"));
                    pendingSelection={ranges:spans,reference:first.label+" · "+first.title,study:true};
                    element("SelectionReference").textContent=pendingSelection.reference;placeSelectionToolbar();element("SelectionToolbar").hidden=false;
                    suppressWordClickUntil=performance.now()+650;swipe=null;return;
                }
            }
            if (performance.now() >= selectionInteractionUntil) clearSelectionActions();
            return;
        }
        const ranges = [];
        for (const container of element("Verses").querySelectorAll(".bs-verse-text")) {
            if (!range.intersectsNode(container)) continue;
            const verse = verseData.get(Number(container.closest(".bs-verse").dataset.verseId));
            function offsetAt(boundary, offset, fallback) {
                if (!container.contains(boundary)) return fallback;
                const prefix = document.createRange(); prefix.selectNodeContents(container); prefix.setEnd(boundary, offset);
                return Array.from(prefix.toString()).length;
            }
            const start = offsetAt(range.startContainer, range.startOffset, 0);
            const end = offsetAt(range.endContainer, range.endOffset, Array.from(verse.text).length);
            if (end > start) ranges.push({verse_id: verse.id, start_offset: start, end_offset: end, selected_text: Array.from(verse.text).slice(start, end).join("")});
        }
        if (ranges.length) { suppressWordClickUntil = performance.now() + 650; swipe = null; showSelectionActions(ranges); }
    }
    let selectionTimer;
    document.addEventListener("selectionchange", () => { clearTimeout(selectionTimer); selectionTimer = setTimeout(captureSelection, 100); });
    element("SelectionToolbar").addEventListener("pointerdown", () => { selectionInteractionUntil = performance.now() + 800; });
    element("DismissSelection").addEventListener("click", () => { window.getSelection()?.removeAllRanges(); clearSelectionActions(); });
    async function copyScripture(text,kind="Scripture") {
        if (navigator.clipboard?.writeText && window.isSecureContext) {
            try { await navigator.clipboard.writeText(text); toast(kind+" copied with its reference."); return; } catch (_error) {}
        }
        // Local Wi-Fi HTTP does not expose the Clipboard API on mobile browsers.
        const fallback = document.createElement("textarea"); fallback.value = text;fallback.readOnly=true;fallback.inputMode="none";
        fallback.style.cssText = "position:fixed;left:0;top:0;opacity:0;font-size:16px"; root.append(fallback);
        fallback.focus(); fallback.select(); fallback.setSelectionRange(0, text.length);
        let copied = false;
        try { copied = document.execCommand("copy"); } catch (_error) {}
        fallback.remove();
        if (copied) toast(kind+" copied with its reference.");
        else {
            element("CopyTitle").textContent="Copy "+kind;element("CopyText").setAttribute("aria-label",kind+" to copy");
            element("CopyText").value = text; element("CopyDialog").showModal();
            element("CopyText").focus(); element("CopyText").select(); element("CopyText").setSelectionRange(0, text.length);
        }
    }
    async function loadPersonal(requestId = chapterRequest) {
        try {
            const data = await api(apiBase + "/personal?" + new URLSearchParams({book: state.book, chapter: state.chapter, translation: state.translation}));
            if (requestId !== chapterRequest) return;
            personal = {annotations: data.annotations || [], bookmarks: data.bookmarks || []};
            studyPersonal = {annotations:data.study_annotations || [],bookmarks:data.study_bookmarks || []};
            paintPersonal();
            paintStudyMarks();
            if (element("SavedDialog").open) renderSavedItems();
        } catch (error) { if (requestId === chapterRequest) toast(error.message); }
    }
    function paintPersonal() {
        for (const paragraph of element("Verses").querySelectorAll(".bs-verse")) {
            const verseId = Number(paragraph.dataset.verseId), container = paragraph.querySelector(".bs-verse-text");
            container.querySelectorAll("mark[data-bs-annotation]").forEach(mark => mark.replaceWith(...mark.childNodes)); container.normalize();
            paragraph.querySelectorAll(".bs-verse-mark").forEach(mark => mark.remove());
            const items = personal.annotations.flatMap(item => (item.ranges || []).filter(span => span.verse_id === verseId).map(span => ({...span, item})));
            const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT), texts = [];
            let text; while ((text = walker.nextNode())) texts.push(text);
            let offset = 0;
            for (const textNode of texts) {
                const chars = Array.from(textNode.textContent), start = offset, end = offset + chars.length; offset = end;
                const hits = items.filter(span => span.start_offset < end && span.end_offset > start);
                if (!hits.length) continue;
                const boundaries = [...new Set([start, end, ...hits.flatMap(span => [Math.max(start, span.start_offset), Math.min(end, span.end_offset)])])].sort((a, b) => a - b);
                const fragment = document.createDocumentFragment();
                for (let i = 0; i < boundaries.length - 1; i++) {
                    const from = boundaries[i], to = boundaries[i + 1], content = chars.slice(from - start, to - start).join("");
                    const active = hits.filter(span => span.start_offset < to && span.end_offset > from);
                    if (!active.length) { fragment.append(document.createTextNode(content)); continue; }
                    const mark = node("mark", content); mark.dataset.bsAnnotation = active[0].item.id;
                    const highlight = active.find(span => span.item.kind === "highlight");
                    mark.style.backgroundColor = highlight?.item.color || (highlight ? "#ffe66d" : "transparent");
                    if (active.some(span => span.item.kind === "underline")) mark.classList.add("bs-underline");
                    mark.title = active.map(span => span.item.note).filter(Boolean).join("\n"); fragment.append(mark);
                }
                textNode.replaceWith(fragment);
            }
            for (const [present, label, symbol] of [[items.length, "Saved highlights and notes", "✎"], [personal.bookmarks.some(item => item.verse_id === verseId), "Saved bookmark", "⚑"]]) {
                if (!present) continue;
                const button = node("button", symbol, "bs-verse-mark"); button.type = "button";
                button.setAttribute("aria-label", label + " for verse " + verseData.get(verseId).verse);
                button.addEventListener("click", () => openSavedItems(verseId)); paragraph.append(button);
            }
        }
    }
    async function createAnnotation(kind, color, note = "") {
        if (!pendingSelection || selectionBusy) return;
        const snapshot = pendingSelection, requestId = chapterRequest;
        selectionBusy = true;
        try {
            await api(apiBase + (snapshot.study ? "/study-saves" : "/annotations"), jsonWrite("POST", {kind, color, note, ranges: snapshot.ranges}));
            if (requestId === chapterRequest && pendingSelection === snapshot) {
                window.getSelection()?.removeAllRanges(); clearSelectionActions();
            }
            if (requestId === chapterRequest) await loadPersonal(requestId);
            toast(kind === "note" ? "Note saved to your global notes & highlights." : "Saved to your global notes & highlights.");
            return true;
        } catch (error) { toast(error.message); return false; }
        finally { selectionBusy = false; }
    }
    function openNote(item = null) {
        editingNote = item;
        element("NoteTitle").textContent = item ? "Edit note" : "Add a note";
        element("NoteReference").textContent = item?.reference || pendingSelection?.reference || "";
        element("NoteText").value = item?.note || ""; element("NoteError").textContent = "";
        element("NoteDialog").showModal(); element("NoteText").focus();
    }
    element("NoteForm").addEventListener("submit", async event => {
        event.preventDefault(); element("SaveNote").disabled = true;
        try {
            if (editingNote) {
                await api(editingNote.delete_url, jsonWrite("PATCH", {note: element("NoteText").value, version: editingNote.version}));
                await loadPersonal(); toast("Note updated.");
            } else if (!await createAnnotation("note", null, element("NoteText").value)) {
                element("NoteError").textContent = element("Toast").textContent; return;
            }
            element("NoteDialog").close();
        } catch (error) { element("NoteError").textContent = error.message; }
        finally { element("SaveNote").disabled = false; }
    });
    element("SelectionToolbar").addEventListener("click", async event => {
        const button = event.target.closest("[data-selection-action]");
        if (!button || !pendingSelection || selectionBusy) return;
        const action = button.dataset.selectionAction;
        if (action === "copy") {
            const snapshot = pendingSelection;
            await copyScripture(snapshot.reference + (snapshot.study ? "" : " (" + translationLabel() + ")") + "\n" + snapshot.ranges.map(span => span.selected_text).join("\n"));
        } else if (action === "share") {
            const snapshot=pendingSelection;
            const text=snapshot.reference+(snapshot.study?"":" ("+translationLabel()+")")+"\n"+
                snapshot.ranges.map(span=>span.selected_text).join("\n");
            button.disabled=true;selectionBusy=true;
            try {
                if(!window.isSecureContext){
                    toast("Open this website using HTTPS to use your phone's share menu. Sharing is unavailable on a local HTTP connection.");return;
                }
                const data={title:snapshot.reference,text};
                if(!navigator.share || (navigator.canShare && !navigator.canShare(data))){
                    toast("This browser cannot open a share menu for this message. Try a supported mobile browser, such as Chrome or Safari.");return;
                }
                // Open directly from the tap, before fetching or saving anything.
                await navigator.share(data);
                toast("Message handed to your phone's share menu. Delivery is handled by your chosen app.");
            } catch(error){
                toast(error.name==="AbortError"?"Sharing cancelled.":"Unable to open the share menu. Please try again.");
            } finally {button.disabled=false;selectionBusy=false;}
        } else if (action === "sermon-notes") {
            const snapshot=pendingSelection;
            const text=snapshot.ranges.map(span=>span.selected_text).join("\n");
            await addToSermonNotes(notesPlainBlocks(text, snapshot.study?"paragraph":"scripture"));
        } else if (action === "note") openNote();
        else if (action === "bookmark") {
            button.disabled = true;
            try {
                const snapshot=pendingSelection;
                await api(apiBase + (snapshot.study?"/study-saves":"/bookmarks"), jsonWrite("POST", snapshot.study
                    ? {kind:"bookmark",note:"",color:null,ranges:[snapshot.ranges[0]]}
                    : {verse_id:snapshot.ranges[0].verse_id,label:snapshot.reference}));
                window.getSelection()?.removeAllRanges(); clearSelectionActions(); await loadPersonal(); toast("Bookmark saved to your global bookmarks.");
            } catch (error) { toast(error.message); }
            finally { button.disabled = false; }
        } else await createAnnotation(action, button.dataset.color || "#e5962d");
    });
    function openSavedItems(verseId = null) {
        window.getSelection()?.removeAllRanges(); clearSelectionActions();
        if (element("SettingsDialog").open) element("SettingsDialog").close();
        savedVerseFilter = verseId; renderSavedItems(); element("SavedDialog").showModal();
    }
    function renderSavedItems() {
        const list = element("SavedItems"); list.replaceChildren();
        const visibleKeys=new Set([...root.querySelectorAll("[data-study-source]")].filter(node=>node.getClientRects().length).map(node=>node.dataset.studySource));
        const items = [...personal.annotations, ...personal.bookmarks,...studyPersonal.annotations,...studyPersonal.bookmarks].filter(item =>
            item.format === "STUDY" ? !savedVerseFilter&&item.ranges.some(span=>visibleKeys.has(span.source_key))
            : !savedVerseFilter || item.verse_id === savedVerseFilter || item.ranges?.some(span => span.verse_id === savedVerseFilter));
        if (!items.length) list.append(node("p", "Tap a verse number or select Scripture to save a highlight, note, or bookmark."));
        for (const item of items) {
            const card = node("section", undefined, "bs-saved-item");
            card.append(node("strong", item.reference + " · " + (item.kind || "bookmark")));
            if (item.selected_text) card.append(node("p", item.selected_text));
            if (item.note) card.append(node("p", item.note));
            const actions = node("div");
            const jump = node("button", "Go to verse"); jump.type = "button";
            if(item.format==="STUDY")jump.textContent="Open source";
            jump.addEventListener("click", () => { element("SavedDialog").close(); item.format==="STUDY"?openSavedStudy(item):jumpToVerse(item.verse); }); actions.append(jump);
            const toNotes=node("button","Add to Sermon Notes");toNotes.type="button";
            toNotes.addEventListener("click",()=>{element("SavedDialog").close();addToSermonNotes(notesPlainBlocks([item.selected_text,item.note].filter(Boolean).join("\n\n")));});actions.append(toNotes,notesCurrentControl());
            if (item.kind) {
                const edit = node("button", "Edit note"); edit.type = "button";
                edit.addEventListener("click", () => { element("SavedDialog").close(); openNote(item); }); actions.append(edit);
            }
            const remove = node("button", "Delete"); remove.type = "button";
            remove.addEventListener("click", async () => {
                remove.disabled = true;
                try { await api(item.delete_url, jsonWrite("DELETE", {})); await loadPersonal(); toast("Saved item deleted."); }
                catch (error) { toast(error.message); remove.disabled = false; }
            }); actions.append(remove); card.append(actions); list.append(card);
        }
    }
    function jumpToVerse(number) {
        if (readerTab !== "bible") selectReaderTab("bible");
        const verse = [...verseData.values()].find(item => item.verse <= number && (item.verse_end || item.verse) >= number);
        const target = element("Verse" + (verse?.verse || number));
        if (!target) return;
        state.verse = number;
        setIllustrationPassage(number,number);
        element("Bible").scrollTop += target.getBoundingClientRect().top - element("Bible").getBoundingClientRect().top - readerChromeHeight() - 14;
        target.classList.add("bs-jump-target"); setTimeout(() => target.classList.remove("bs-jump-target"), 2500);
    }

    async function openSavedStudy(item) {
        const dialog=element("SavedStudyDialog"),content=element("SavedStudyContent");
        content.replaceChildren();element("SavedStudyTitle").textContent=item.reference+" · "+item.book_author;
        element("SavedStudyNotice").textContent="Loading original source…";
        dialog.dataset.studyKind=item.ranges[0].source.category;
        if(!dialog.open)dialog.showModal();
        const source=item.ranges[0].source;
        try {
            const data=await api(source.origin);
            const current=(data.study_anchors||[]).find(anchor=>anchor.key===source.key)||(data.anchor?.key===source.key?data.anchor:null);
            if(!current)throw Error("The source has changed. Your saved excerpt is preserved below.");
            if(data.image) {
                const image=node("img");image.src=data.image.url;image.alt=data.image.text;image.className="bs-chain-chart";content.append(image);
                const caption=node("p",data.image.text);bindStudyText(caption,data.anchor);content.append(caption);
            }
            const blockIndex=source.path.lastIndexOf("blocks");
            let blocks=null;
            if(blockIndex>=0) {
                let value=data;
                for(const part of source.path.slice(0,blockIndex+1))value=value?.[part];
                if(Array.isArray(value))blocks=value;
            }
            if(blocks)appendSourceBlocks(content,blocks);
            else for(const anchor of data.study_anchors||[]) {
                if(anchor.title!==source.title)continue;
                const paragraph=node(anchor.block_type==="heading"?"h5":"p",anchor.text);
                bindStudyText(paragraph,anchor);content.append(paragraph);
            }
            element("SavedStudyNotice").textContent="Original indexed source · "+source.title;
        } catch(error) {element("SavedStudyNotice").textContent=error.message+" Showing your saved text.";}
        // Preserve the saved excerpt even when a preview and its full paragraph differ.
        for(const span of item.ranges) {
            if([...content.querySelectorAll("[data-study-source]")].some(node=>node.dataset.studySource===span.source_key))continue;
            const excerpt=node("p",span.block_text);
            const anchor={...span.source,text:span.block_text,key:span.source_key,token:span.token};
            const fresh=studyAnchors.get(span.source.category+"|"+span.block_text);
            if(fresh&&fresh.key===span.source_key)bindStudyText(excerpt,fresh);
            else {excerpt.dataset.studySource=span.source_key;boundStudyAnchors.set(excerpt,anchor);}
            content.append(excerpt);
        }
        paintStudyMarks();
        const target=[...content.querySelectorAll("[data-study-source]")].find(node=>node.dataset.studySource===source.key);
        if(target)requestAnimationFrame(()=>target.scrollIntoView({block:"center"}));
    }

    function loadWord(verseId,tokenIndex,part=selectedWord?.part || "auto"){return visibleStudy(()=>fetchWord(verseId,tokenIndex,part));}
    async function fetchWord(verseId, tokenIndex, part="auto") {
        const requestId = ++wordRequest;
        const results = element("WordResults");
        results.replaceChildren(loadingNotice("Uncovering meaning…", "Exploring this word in your study library."));
        try {
            const data = await api(apiBase + "/word/" + verseId + "/" + tokenIndex + (part!=="auto" ? "?part="+part : ""));
            if (requestId !== wordRequest) return;
            const query=data.query || data.word;
            element("Search").value = element("ConcordanceSearch").value = query;
            if(selectedWord?.verseId===verseId&&selectedWord.tokenIndex===tokenIndex){selectedWord.word=query;selectedWord.part=part;}
            if(data.lookup_token_indices?.length){
                const indices=new Set(data.lookup_token_indices);
                root.querySelectorAll('[data-verse-id="'+verseId+'"] .bs-word').forEach(word=>word.classList.toggle("is-selected",indices.has(Number(word.dataset.tokenIndex))));
            }
            if(data.result_type==="native_dictionary"){
                renderNativeDictionaryResults(data,requestId);return;
            }
            if (data.verified !== true && data.result_type !== "dictionary") {
                results.replaceChildren(node("h3", data.word), node("p", "No verified original-language equivalent is available for this word in this verse."));
                renderPeople(data.people, requestId);
                return;
            }
            renderDictionaryResults(data, requestId);
            renderPeople(data.people, requestId);
            if (data.verified !== true) return;
            const original = node("section", undefined, "bs-original-result");
            original.append(node("h4", "Verified original-language alignment"));
            for (const token of data.original_tokens || []) {
                const card = node("article", undefined, "bs-entry");
                const spelling = node("h4", token.spelling); spelling.dir = "auto";
                card.append(spelling, node("p", token.language + " · " + token.source_ref, "bs-attribution"));
                for (const [label, value] of [["Transliteration", token.transliteration], ["Lemma", token.lemma], ["Morphology", token.morphology]]) {
                    if (value) card.append(node("p", label + ": " + value));
                }
                for (const identifier of token.identifiers || []) {
                    card.append(node("p", "STEP " + identifier.extended_strong + (identifier.standard_strong ? " · Strong’s " + identifier.standard_strong : "")));
                    if (identifier.definition) card.append(node("p", identifier.definition));
                }
                original.append(card);
            }
            results.append(original);
        } catch (error) {
            if (requestId === wordRequest) results.replaceChildren(node("p", error.message));
        }
    }

    async function previewGreek(link, block, sourceBlocks) {
        const requestId=++greekRequest, source=block.greek_source;
        const spelling=Array.from(block.text).slice(link.start,link.end).join("");
        element("GreekTitle").textContent=spelling;
        element("GreekAttribution").textContent=source.source_title+" · "+source.passage+" · Pulpit’s surrounding explanation";
        const content=element("GreekContent"), actions=element("GreekActions");
        content.replaceChildren();actions.replaceChildren();
        const blocks=sourceBlocks || [block], index=Math.max(0,blocks.indexOf(block));
        // EPUB paragraphs are complete; PDF explanations can continue over lines/pages.
        let first=index, last=index+1, size=block.text.length;
        if (size<500) {
            while(first>0 && index-first<3 && size<1100){first--;size+=blocks[first].text.length;}
            while(last<blocks.length && last-index<16 && size<1900){size+=blocks[last++].text.length;}
        }
        for (const item of blocks.slice(first,last)) content.append(renderSourceBlock({...item,greek_links:[]}));
        paintStudyMarks();
        element("GreekDialog").showModal();
        try {
            const query=new URLSearchParams({word:spelling,book:books[source.book_id-1].name,chapter:source.chapter,verse:source.verse});
            const data=await api(apiBase+"/pulpit-greek?"+query);
            if(requestId!==greekRequest || !element("GreekDialog").open || !data.match)return;
            const button=node("button","Open Word Study · "+data.match.lemma);button.type="button";
            button.addEventListener("click",()=>{
                element("GreekDialog").close();element("Search").value=data.match.query;
                selectedWord={word:data.match.query};
                if(studyTab==="word")element("SearchForm").requestSubmit();else selectStudyTab("word");
            });actions.append(button);
        } catch (_) { /* The source explanation remains available without dictionary data. */ }
    }
    element("CloseGreek").addEventListener("click",()=>element("GreekDialog").close());
    element("GreekDialog").addEventListener("close",()=>{
        // A queued close event from a previous opening must not invalidate a
        // newer lookup if the same dialog has already been reopened.
        if(!element("GreekDialog").open)++greekRequest;
    });

    function renderDictionaryPreview(data,scrollTop=0){
        currentDictionaryPreview=data;
        element("DictionaryTitle").textContent=data.title;
        element("DictionaryBack").hidden=!dictionaryHistory.length;element("DictionaryBack").disabled=false;
        const content=element("DictionaryContent");content.replaceChildren();
        for(const entry of data.entries){
            const article=node("article");
            const expository=["mounce","vine"].includes(entry.source);
            if(expository){article.classList.add("bs-expository");article.dataset.dictionaryFamily=entry.source;}
            if(data.entries.length>1||expository)article.append(node("h4",entry.headword,"bs-dictionary-title"));
            article.append(node("p",[entry.source_title,entry.section,entry.language].filter(Boolean).join(" · "),"bs-attribution"));
            appendSourceBlocks(article,entry.context_blocks || []);
            appendSourceBlocks(article,entry.blocks || []);
            if(entry.attribution)article.append(node("p",entry.attribution,"bs-attribution"));
            if(entry.rights_note)article.append(node("p",entry.rights_note,"bs-attribution"));
            content.append(article);
        }
        element("DictionaryDialog").scrollTop=scrollTop;
    }
    async function previewDictionaryReference(link){
        const ids=[...new Set(link.entry_ids || [])].filter(id=>Number.isSafeInteger(id)&&id>0);
        if(!ids.length)return;
        const dialog=element("DictionaryDialog"),requestId=++dictionaryPreviewRequest;
        const previous=dialog.open&&currentDictionaryPreview ? {data:currentDictionaryPreview,scrollTop:dialog.scrollTop} : null;
        if(!dialog.open){dictionaryHistory.length=0;currentDictionaryPreview=null;}
        element("DictionaryTitle").textContent=link.headword;
        element("DictionaryBack").disabled=true;
        element("DictionaryBack").hidden=!dictionaryHistory.length&&!previous;
        element("DictionaryContent").replaceChildren(loadingNotice("Uncovering meaning…", "Opening your dictionary article."));
        if(!dialog.open)dialog.showModal();
        try{
            const entries=await Promise.all(ids.map(async id=>(await api(apiBase+"/dictionaries/entry/"+id)).entry));
            if(requestId!==dictionaryPreviewRequest||!dialog.open)return;
            if(previous){dictionaryHistory.push(previous);if(dictionaryHistory.length>20)dictionaryHistory.shift();}
            renderDictionaryPreview({title:link.headword,entries});
        }catch(error){
            if(requestId!==dictionaryPreviewRequest||!dialog.open)return;
            if(previous){renderDictionaryPreview(previous.data,previous.scrollTop);element("DictionaryContent").prepend(node("p",error.message));}
            else{
                const retry=node("button","Retry");retry.type="button";retry.addEventListener("click",()=>previewDictionaryReference(link));
                element("DictionaryContent").replaceChildren(node("p",error.message),retry);
            }
        }
    }
    element("DictionaryBack").addEventListener("click",()=>{
        const previous=dictionaryHistory.pop();
        if(previous){++dictionaryPreviewRequest;renderDictionaryPreview(previous.data,previous.scrollTop);}
    });
    element("CloseDictionary").addEventListener("click",()=>element("DictionaryDialog").close());
    element("DictionaryDialog").addEventListener("close",()=>{++dictionaryPreviewRequest;currentDictionaryPreview=null;dictionaryHistory.length=0;});

    function renderSourceBlock(block, sourceBlocks=null) {
        if (block.type === "table") {
            const wrapper=node("div",undefined,"bs-source-table-wrap"), table=node("table",undefined,"bs-source-table");
            for(const row of block.rows || []) {
                const tr=node("tr");
                for(const cell of row) {
                    const td=node(cell.header ? "th" : "td");
                    td.colSpan=cell.colspan || 1;td.append(renderSourceBlock(cell.block,sourceBlocks));tr.append(td);
                }
                table.append(tr);
            }
            wrapper.append(table);return wrapper;
        }
        const paragraph = node(block.role === "commentary_heading" && block.level > 1 ? "h6" : block.type === "heading" ? "h5" : "p");
        if(block.illustration)paragraph.classList.add("bs-illustration");
        if(["testament","section","numbered","paragraph"].includes(block.dictionary_style))paragraph.classList.add("bs-dictionary-"+block.dictionary_style);
        if(block.presentation){
            if(["center","right","left","justify"].includes(block.presentation.align))paragraph.style.textAlign=block.presentation.align;
            if(Number.isFinite(block.presentation.indent))paragraph.style.textIndent=block.presentation.indent+"em";
            if(Number.isFinite(block.presentation.indent_percent)&&Math.abs(block.presentation.indent_percent)<=15)paragraph.style.textIndent=block.presentation.indent_percent+"%";
            if(Number.isFinite(block.presentation.inset))paragraph.style.marginLeft=block.presentation.inset+"em";
        }
        if (block.role === "major_chain") paragraph.classList.add("bs-major-chain");
        if (["commentary_main_idea", "commentary_outline", "commentary_outline_continuation", "commentary_heading", "commentary_reference", "commentary_section", "sermon_heading", "sermon_subpoint", "sermon_reference", "sermon_paragraph", "sermon_list_item"].includes(block.role)) {
            paragraph.classList.add("bs-" + block.role.replaceAll("_", "-"));
            if ([1, 2, 3].includes(block.level)) paragraph.classList.add("bs-commentary-level-" + block.level);
            if (block.marker) paragraph.append(node("span", block.marker + " ", "bs-commentary-marker"));
        }
        if (block.source_quote) paragraph.classList.add("bs-source-quote");
        const sourceText=node("span");paragraph.append(sourceText);
        const runs = block.runs || [{type: "text", text: block.text, marks: []}];
        let position = 0;
        const positioned = runs.map(run => {
            const size = run.type === "image" ? 1 : Array.from(run.text).length;
            const item = {run, start: position, end: position + size}; position += size; return item;
        });
        function appendSlice(parent, from, to) {
            for (const item of positioned) {
                if (item.end <= from || item.start >= to) continue;
                let content;
                if (item.run.type === "image") {
                    content = document.createElement("img");
                    content.src = apiBase + "/dictionaries/asset/" + item.run.asset;
                    content.alt = "Source spelling or symbol"; content.className = "bs-source-image";
                    if(Number.isFinite(item.run.height_em)&&item.run.height_em>=.15&&item.run.height_em<=3){content.classList.add("bs-inline-glyph");content.style.height=item.run.height_em+"em";}
                } else {
                    const characters = Array.from(item.run.text);
                    content = document.createTextNode(characters.slice(Math.max(0, from - item.start), Math.min(characters.length, to - item.start)).join(""));
                }
                for (const mark of (item.run.marks || [])) {
                    if(mark === "smallcaps") {const wrapper=node("span",undefined,"bs-smallcaps");wrapper.append(content);content=wrapper;continue;}
                    if (!["em", "strong", "sup", "sub", "u"].includes(mark)) continue;
                    const wrapper = document.createElement(mark); wrapper.append(content); content = wrapper;
                }
                parent.append(content);
            }
        }
        let offset = 0;
        const links = [...(block.references || []), ...(block.chain_links || []).map(link => ({...link, chain: true})),
            ...(block.greek_links || []).map(link=>({...link,greek:true})),
            ...(block.dictionary_links || []).map(link=>({...link,dictionary:true}))].sort((a, b) => a.start - b.start);
        for (const reference of links) {
            if (reference.start < offset || reference.end > position) continue;
            appendSlice(sourceText, offset, reference.start);
            const button = node("button", undefined, reference.dictionary ? "bs-dictionary-ref" : reference.greek ? "bs-greek-ref" : reference.chain ? "bs-chain-ref" : "bs-scripture-ref"); button.type = "button";
            appendSlice(button, reference.start, reference.end);
            let pressedAt=0;
            button.addEventListener("pointerdown",()=>{pressedAt=performance.now();});
            button.addEventListener("click", event => {
                if(reference.greek && (performance.now()<suppressWordClickUntil || window.getSelection()?.toString().trim()
                    || (event.detail && pressedAt && performance.now()-pressedAt>450)))return;
                if(reference.dictionary)previewDictionaryReference(reference);
                else if(reference.greek)previewGreek(reference,block,sourceBlocks);
                else if(reference.chapter_only)navigatePassage(reference.book,reference.chapter,1,true);
                else if(reference.chain)previewChain(reference.topic_id,true,reference.key);
                else previewVerse(reference);
            });
            sourceText.append(button); offset = reference.end;
        }
        appendSlice(sourceText, offset, position);
        if(bindStudyText(sourceText,block._study_texts?.text))paintStudyContainer(sourceText);
        return paragraph;
    }

    function appendSourceBlocks(parent, blocks) {
        const lists = [];
        for (const block of blocks) {
            if (!["bullet", "ordered"].includes(block.list_kind)) {
                lists.length = 0; parent.append(renderSourceBlock(block,blocks)); continue;
            }
            const level = Math.max(1, Math.min(6, Number(block.list_level) || 1));
            while (lists.length > level) lists.pop();
            if (lists.length === level && lists[level - 1].kind !== block.list_kind) lists.pop();
            while (lists.length < level) {
                const owner = lists.length ? lists[lists.length - 1] : null;
                if (owner && !owner.item) { owner.item = node("li"); owner.list.append(owner.item); }
                const list = node(block.list_kind === "ordered" ? "ol" : "ul", undefined, "bs-source-list");
                if (block.list_kind === "ordered") {
                    if (Number.isInteger(block.list_number)) list.start = block.list_number;
                    if (["1", "a", "A", "i", "I"].includes(block.list_style)) list.type = block.list_style;
                }
                (owner ? owner.item : parent).append(list);
                lists.push({list, kind: block.list_kind, item: null, key: null});
            }
            const current = lists[level - 1];
            if (!current.item || current.key !== block.list_item || block.list_item == null) {
                current.item = node("li"); current.key = block.list_item;
                if (block.list_kind === "ordered" && Number.isInteger(block.list_number)) current.item.value = block.list_number;
                current.list.append(current.item);
            }
            current.item.append(renderSourceBlock(block,blocks));
        }
    }

    function dictionaryEntryCard(entry, ownerRequest) {
        const card = node("article", undefined, "bs-definition");
        const title=node("h4",entry.headword,"bs-dictionary-title");
        card.append(title, node("p", [entry.part ? entry.part[0].toUpperCase()+entry.part.slice(1) : "", entry.section, entry.language].filter(Boolean).join(" · "), "bs-attribution"));
        if(entry.match_type==="article_text")card.append(node("p","“"+entry.matched_query+"” appears in this article.","bs-attribution"));
        if (entry.evidence?.length) card.append(node("p", entry.evidence.join(" · "), "bs-evidence"));
        if (entry.identifiers?.length) card.append(node("p", entry.identifiers.filter(item => item.scheme === "strong").map(item => (item.language === "Hebrew" ? "H" : "G") + item.number + item.qualifier).join(" · "), "bs-attribution"));
        const preview = renderSourceBlock(entry.preview_block || {type: "paragraph", text: entry.preview, references: entry.preview_references || []});
        card.append(preview);
        const full = node("div", undefined, "bs-source-content"); full.hidden = true; card.append(full);
        const more = node("button", "Read more", "bs-show-more"); more.type = "button";
        more.setAttribute("aria-expanded", "false"); card.append(more);
        function expositoryLayout(family){
            if(!["mounce","vine"].includes(family))return;
            card.classList.add("bs-expository");card.dataset.dictionaryFamily=family;
        }
        expositoryLayout(entry.source);
        let loaded = false;
        more.addEventListener("click", async () => {
            if (!full.hidden) {
                full.hidden = true; preview.hidden = false; more.textContent = "Read more"; more.setAttribute("aria-expanded", "false"); return;
            }
            more.disabled = true;
            try {
                if (!loaded) {
                    const data = await api(apiBase + "/dictionaries/entry/" + entry.id);
                    if (ownerRequest !== wordRequest) return;
                    const source = data.entry;
                    expositoryLayout(source.source);
                    full.append(node("p", source.source_title, "bs-attribution"));
                    if(source.attribution && source.attribution!==source.source_title)full.append(node("p",source.attribution,"bs-attribution"));
                    if(source.rights_note)full.append(node("p",source.rights_note,"bs-attribution"));
                    if (source.identifiers.length) {
                        const identifiers = source.identifiers.map(identifier => (identifier.scheme === "strong"
                            ? (identifier.language === "Hebrew" ? "H" : "G") : "GK " + identifier.language + " ")
                            + identifier.number + identifier.qualifier);
                        full.append(node("p", [...new Set(identifiers)].join(" · "), "bs-attribution"));
                    }
                    appendSourceBlocks(full,source.context_blocks || []);
                    appendSourceBlocks(full,source.blocks);
                    if (!source.blocks.length) full.append(node("p", "This source lists the heading without a separate definition."));
                    loaded = true;
                }
                full.hidden = false; preview.hidden = true; more.textContent = "Show less"; more.setAttribute("aria-expanded", "true");
            } catch (error) {
                full.replaceChildren(node("p", error.message)); full.hidden = false;
                more.textContent="Retry";
            } finally {
                more.disabled = false;
            }
        });
        return card;
    }

    function translationChoices(translation, ownerRequest) {
        const section=node("section",undefined,"bs-translations");
        section.setAttribute("aria-label",translation.label+" possible equivalents");
        const line=node("p",undefined,"bs-translation-choices");
        line.append(node("strong","("+translation.label+") ","bs-translation-label"));section.append(line);
        if(!translation.available){line.append(document.createTextNode("This dictionary has not been synced yet."));return section;}
        if(!translation.entries.length){line.append(document.createTextNode("No direct dictionary match found."));return section;}
        const panel=node("div",undefined,"bs-translation-definition");panel.hidden=true;
        panel.id="bsTranslationDefinition"+ownerRequest+translation.label.replace(/[^a-z]/gi,"");
        const buttons=new Map(),cache=new Map();let active=null,revision=0;
        const close=()=>{
            ++revision;active=null;panel.hidden=true;panel.removeAttribute("aria-busy");
            for(const button of buttons.values())button.setAttribute("aria-expanded","false");
        };
        async function show(entry,retry=false){
            const ids=entry.entry_ids || [entry.id],key=ids.join(",");
            if(active===key&&!retry){close();return;}
            active=key;const request=++revision;
            for(const [candidate,button] of buttons)button.setAttribute("aria-expanded",String(candidate===key));
            panel.hidden=false;panel.setAttribute("aria-busy","true");
            const dismiss=node("button","Close","bs-translation-close");dismiss.type="button";
            dismiss.setAttribute("aria-label","Close "+translation.label+" definition");dismiss.addEventListener("click",close);
            const content=node("div",undefined,"bs-source-content");content.append(loadingNotice("Uncovering meaning…", "Preparing this definition."));
            panel.replaceChildren(dismiss,node("h4",entry.headword),content);
            try{
                let sources=cache.get(key);
                if(!sources){
                    sources=await Promise.all(ids.map(async id=>(await api(apiBase+"/dictionaries/entry/"+id)).entry));
                    cache.set(key,sources);
                }
                if(request!==revision||ownerRequest!==wordRequest||!section.isConnected)return;
                content.replaceChildren();
                for(const source of sources){
                    content.append(node("p",[source.section,source.language].filter(Boolean).join(" · "),"bs-attribution"));
                    appendSourceBlocks(content,source.blocks || []);
                    if(source.attribution)content.append(node("p",source.attribution,"bs-attribution"));
                    if(source.rights_note)content.append(node("p",source.rights_note,"bs-attribution"));
                }
                paintStudyMarks();
            }catch(error){
                if(request!==revision||ownerRequest!==wordRequest)return;
                const again=node("button","Retry");again.type="button";again.addEventListener("click",()=>show(entry,true));
                content.replaceChildren(node("p",error.message),again);
            }finally{if(request===revision)panel.removeAttribute("aria-busy");}
        }
        translation.entries.forEach((entry,index)=>{
            if(index)line.append(document.createTextNode(", "));
            const button=node("button",entry.headword,"bs-translation-choice");button.type="button";
            button.title=entry.preview || "Open full dictionary entry";
            button.setAttribute("aria-expanded","false");button.setAttribute("aria-controls",panel.id);
            button.addEventListener("click",()=>show(entry));buttons.set((entry.entry_ids || [entry.id]).join(","),button);line.append(button);
        });
        section.append(panel);return section;
    }

    function renderNativeDictionaryResults(data,ownerRequest,originWord=null){
        const results=element("WordResults"),dictionary=data.dictionary;
        results.replaceChildren(node("h3",originWord || data.query || data.word),node("p",dictionary.label+" dictionary","bs-kicker"));
        if(originWord&&originWord!==data.query)results.append(node("p","Dictionary spelling: "+data.query));
        results.append(node("p",data.accuracy_notice,"bs-notice bs-native-notice"));
        if(!dictionary.available){results.append(node("p","This dictionary has not been synced yet."));return;}
        if(!dictionary.entries.length){
            results.append(node("p","No exact dictionary entry found. Try a base form or another spelling."));
            if(data.suggestions?.length){
                results.append(node("p","Possible dictionary spellings — these are suggestions, not confirmed base forms.","bs-attribution"));
                const choices=node("div",undefined,"bs-native-spellings");
                for(const suggestion of data.suggestions){
                    const button=node("button",suggestion.word);button.type="button";
                    button.addEventListener("click",()=>{if(ownerRequest===wordRequest){element("Search").value=suggestion.word;searchDictionaryWord(suggestion.word,"auto",originWord || data.query);}});
                    const candidate=node("div",undefined,"bs-native-spelling");candidate.append(button);
                    if(suggestion.preview)candidate.append(node("p",suggestion.preview,"bs-attribution"));
                    choices.append(candidate);
                }
                results.append(choices);
            }
            return;
        }
        const englishArea=node("section",undefined,"bs-native-english");englishArea.hidden=true;
        englishArea.append(node("h4","English dictionary study"),node("p",data.accuracy_notice,"bs-notice bs-native-notice"));
        const englishResults=node("div");englishArea.append(englishResults);
        let englishRequest=0;const englishButtons=[];
        async function lookupEnglish(choice,part=choice.part || "all"){
            const revision=++englishRequest,finish=beginStudyLoad();
            englishArea.hidden=false;englishResults.replaceChildren(loadingNotice("Uncovering meaning…", "Exploring “"+choice.word+"” in your dictionaries."));
            for(const item of englishButtons)item.button.setAttribute("aria-pressed",String(item.choice===choice));
            try{
                const page=await api(apiBase+"/dictionaries/search?"+new URLSearchParams({q:choice.word,part}));
                if(ownerRequest!==wordRequest||revision!==englishRequest||!results.isConnected)return;
                renderDictionaryResults({...page,translations:[]},ownerRequest,englishResults,value=>lookupEnglish(choice,value));
                renderPeople(page.people,ownerRequest,englishResults);
            }catch(error){if(ownerRequest===wordRequest&&revision===englishRequest)englishResults.replaceChildren(node("p",error.message));}
            finally{finish();}
        }
        for(const entry of dictionary.entries){
            const section=node("section",undefined,"bs-native-entry");
            if(entry.is_recorded_form)section.append(node("p","Recorded dictionary form of “"+entry.headword+"”.","bs-attribution"));
            section.append(dictionaryEntryCard(entry,ownerRequest));
            if(entry.english_equivalents?.length){
                const toggle=node("button","Explore English equivalents");toggle.type="button";toggle.setAttribute("aria-expanded","false");
                const choices=node("div",undefined,"bs-native-equivalents");choices.hidden=true;
                choices.append(node("p","Choose the meaning that fits this verse:","bs-attribution"));
                for(const choice of entry.english_equivalents){
                    const button=node("button",choice.word,"bs-english-equivalent");button.type="button";button.title=choice.meaning;button.setAttribute("aria-pressed","false");
                    button.addEventListener("click",()=>{if(ownerRequest===wordRequest)lookupEnglish(choice);});
                    englishButtons.push({button,choice});choices.append(button);
                }
                toggle.addEventListener("click",()=>{choices.hidden=!choices.hidden;toggle.setAttribute("aria-expanded",String(!choices.hidden));});
                section.append(toggle,choices);
            }else section.append(node("p","No short English equivalent is listed for this entry. Read the full definition.","bs-attribution"));
            results.append(section);
        }
        results.append(englishArea);
    }

    function renderDictionaryResults(data, ownerRequest, results=element("WordResults"), onTypeChange=null) {
        results.replaceChildren(node("h3", data.query || data.word));
        if(data.word_type){
            const labels={all:"All word types",name:"Person / name",noun:"Noun",verb:"Verb",adjective:"Adjective",adverb:"Adverb",grammar:"Grammar word"};
            const type=data.word_type,control=node("div",undefined,"bs-word-type"),label=node("label","Word type for equivalents ");
            const select=node("select");select.setAttribute("aria-label","Word type");
            for(const [value,text] of [["auto","Auto"],...Object.entries(labels)]){const option=node("option",text);option.value=value;select.append(option);}
            select.value=type.basis==="chosen" ? type.part : "auto";
            label.append(select);control.append(label,node("span",(type.grammar_kind || labels[type.part] || "All word types")+" · "+type.basis,"bs-attribution"));
            select.addEventListener("change",()=>{
                if(ownerRequest!==wordRequest)return;
                if(onTypeChange)onTypeChange(select.value);
                else if(selectedWord?.verseId!==undefined)loadWord(selectedWord.verseId,selectedWord.tokenIndex,select.value);
                else searchDictionaryWord(data.query || data.word,select.value);
            });
            results.append(control);
            if(type.part==="grammar"){
                const notes={article:"Introduces a noun; “the” usually identifies something specific, while “a” and “an” introduce a nonspecific item.",pronoun:"Refers to a person or thing. Its reference depends on the surrounding sentence.",conjunction:"Connects words or clauses and helps show how their ideas relate.",preposition:"Expresses a relationship such as place, direction, time, or association.",determiner:"Identifies or limits the noun that follows.","auxiliary verb":"Helps express tense, possibility, obligation, or a question. Some of these words also function as main verbs.","preposition / infinitive marker":"Can introduce a relationship or a verb in its infinitive form. Read the surrounding phrase to distinguish the uses."};
                results.append(node("p",notes[type.grammar_kind] || "Study this word through its role in the sentence and any grammatical definitions below.","bs-grammar-note"));
            }
        }
        for(const translation of data.translations || [])results.append(translationChoices(translation,ownerRequest));
        const sourceOrder=["harper","vine","mounce"];
        for (const group of [...data.sources].sort((a,b)=>sourceOrder.indexOf(a.source)-sourceOrder.indexOf(b.source))) {
            const section = node("section", undefined, "bs-dictionary");
            section.dataset.dictionarySource=group.source;
            const toggle = node("button", group.label, "bs-source-toggle"); toggle.type = "button"; toggle.setAttribute("aria-expanded", "true");
            const body = node("div");
            toggle.addEventListener("click", () => { body.hidden = !body.hidden; toggle.setAttribute("aria-expanded", String(!body.hidden)); });
            section.append(toggle, body); results.append(section);
            if (!group.available) body.append(node("p", "This dictionary has not been synced yet."));
            else if (!group.entries.length) body.append(node("p", group.source==="harper" && data.word_type?.part && data.word_type.part!=="all" ? "No direct definition for this word type in this book." : "No matching entry found in this book."));
            for(const entry of group.entries)body.append(dictionaryEntryCard(entry, ownerRequest));
            if(group.other_entries?.length){
                const details=node("details",undefined,"bs-other-definitions");details.append(node("summary","Other word types / unclassified senses"));
                for(const entry of group.other_entries)details.append(dictionaryEntryCard(entry,ownerRequest));
                body.append(details);
            }
            if(group.related_entries?.length){
                const details=node("details",undefined,"bs-related-articles");details.append(node("summary","Related articles mentioning this word"));
                details.append(node("p","These are article mentions, rather than direct definitions.","bs-attribution"));
                for(const entry of group.related_entries)details.append(dictionaryEntryCard(entry,ownerRequest));
                body.append(details);
                if(group.related_next_offset!=null){
                    const more=node("button","More related articles");more.type="button";details.append(more);let offset=group.related_next_offset;
                    more.addEventListener("click",async()=>{
                        more.disabled=true;
                        try{
                            const params=new URLSearchParams({q:data.query,source:group.source,offset,related:1,part:data.word_type?.part || "all"});
                            const page=await api(apiBase+"/dictionaries/search?"+params);
                            if(ownerRequest!==wordRequest)return;
                            for(const entry of page.sources[0].related_entries)details.insertBefore(dictionaryEntryCard(entry,ownerRequest),more);
                            offset=page.sources[0].related_next_offset;more.hidden=offset===null;
                        }catch(error){more.textContent=error.message;}finally{more.disabled=false;}
                    });
                }
            }
            if (group.next_offset != null) {
                const next = node("button", "More matching entries"); next.type = "button"; body.append(next);
                let offset = group.next_offset;
                next.addEventListener("click", async () => {
                    next.disabled = true;
                    try {
                        const query = new URLSearchParams({q: data.query, source: group.source, offset, part:data.word_type?.part || "all"});
                        const page = await api(apiBase + "/dictionaries/search?" + query);
                        if (ownerRequest !== wordRequest) return;
                        for (const entry of page.sources[0].entries)body.insertBefore(dictionaryEntryCard(entry, ownerRequest), next);
                        offset = page.sources[0].next_offset;
                        next.hidden = offset === null;
                    } catch (error) { next.textContent = error.message; }
                    finally { next.disabled = false; }
                });
            }
        }
    }

    element("SearchForm").addEventListener("submit", async event => {
        event.preventDefault();
        const query = element("Search").value.trim();
        if (!query) return;
        await searchDictionaryWord(query);
    });

    async function searchDictionaryWord(query,part="auto",nativeOrigin=null) {
        const finish=beginStudyLoad();
        selectedWord = {word: query,part,nativeOrigin}; element("ConcordanceSearch").value = query;
        ++concordanceRequest;
        const requestId = ++wordRequest;
        element("WordResults").replaceChildren(loadingNotice("Uncovering meaning…", "Exploring your dictionaries for “" + query + "”."));
        try {
            const data = await api(apiBase + "/dictionaries/search?" + new URLSearchParams({q: query,part,translation:state.translation}));
            if (requestId === wordRequest) {
                if(data.result_type==="native_dictionary")renderNativeDictionaryResults(data,requestId,nativeOrigin);
                else {renderDictionaryResults(data, requestId); renderPeople(data.people, requestId);}
            }
        } catch (error) {
            if (requestId === wordRequest) element("WordResults").replaceChildren(node("p", error.message));
        } finally{finish();}
    }

    function selectStudyTab(name, focus = false) {
        if (!["word", "concordance", "commentary", "sermon", "illustrations"].includes(name)) return;
        const changed = studyTab !== name;
        if (changed) { clearCommentaryVerseFocus();++wordRequest; ++concordanceRequest; ++commentaryRequest; ++illustrationRequest; }
        studyTab = name;
        cancelBackgroundStudy();
        if(["commentary","sermon"].includes(name))preferredStudy=name;
        for (const [tab, prefix] of [["word", "Word"], ["concordance", "Concordance"], ["commentary", "Commentary"], ["sermon", "Sermon"], ["illustrations","Illustrations"]]) {
            const active = tab === name;
            element(prefix + "Tab").setAttribute("aria-selected", String(active));
            element(prefix + "Tab").tabIndex = active ? 0 : -1;
            element(prefix + "Panel").hidden = !active;
        }
        positionWorkspaceTools();
        if (focus) element(({word:"Word",concordance:"Concordance",commentary:"Commentary",sermon:"Sermon",illustrations:"Illustrations"})[name]+"Tab").focus();
        if (name === "illustrations") {loadIllustrations();return;}
        if (name === "sermon") { loadSermons(); return; }
        if (name === "commentary") { loadCommentary(); return; }
        if (!changed || !selectedWord) {scheduleBackgroundStudy();return;}
        element("Search").value = element("ConcordanceSearch").value = selectedWord.word;
        if (name === "concordance") loadConcordance(selectedWord.word);
        else if (selectedWord.verseId !== undefined) loadWord(selectedWord.verseId, selectedWord.tokenIndex);
        else searchDictionaryWord(selectedWord.word,selectedWord.part || "auto",selectedWord.nativeOrigin);
    }
    root.querySelectorAll("[data-study-tab]").forEach(button => {
        button.addEventListener("click", () => selectStudyTab(button.dataset.studyTab));
        button.addEventListener("keydown", event => {
            if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
            event.preventDefault();
            const tabs = ["word", "concordance", "commentary", "sermon", "illustrations"];
            const next = (tabs.indexOf(studyTab) + (event.key === "ArrowLeft" ? -1 : 1) + tabs.length) % tabs.length;
            selectStudyTab(event.key === "Home" ? tabs[0] : event.key === "End" ? tabs.at(-1) : tabs[next], true);
        });
    });

    let sermonListScroll=0;
    function sermonDisplayTitle(title) {
        return String(title || "").toLocaleLowerCase().replace(/\p{L}[\p{L}\p{M}'’]*/gu,
            word=>/^[ivx]+$/.test(word)?word.toLocaleUpperCase():word[0].toLocaleUpperCase()+word.slice(1));
    }
    function loadSermons(){return visibleStudy(fetchSermons);}
    async function fetchSermons() {
        const chapter=state.book+" "+state.chapter;
        if(sermonChapter===chapter)return;
        const requestId=++sermonRequest;
        element("SermonReference").textContent=chapter;
        element("SermonReader").hidden=true;element("SermonResults").hidden=false;
        element("SermonResults").replaceChildren(loadingNotice("Shaping the message…", "Finding sermon outlines for " + chapter + "."));
        try {
            await chapterLoadPromise;
            if(requestId!==sermonRequest || studyTab!=="sermon")return;
            const data=await api(apiBase+"/sermons?"+new URLSearchParams({book:state.book,chapter:state.chapter}));
            if(requestId!==sermonRequest)return;
            sermonChapter=chapter;element("SermonResults").replaceChildren();
            if(!data.sermons?.length)element("SermonResults").append(node("p","No indexed sermon or teaching outline was found for this chapter."));
            for(const sermon of data.sermons||[]) {
                const button=node("button",sermonDisplayTitle(sermon.title),"bs-sermon-title");button.type="button";
                button.addEventListener("click",()=>openSermon(sermon.id));element("SermonResults").append(button);
            }
        } catch(error){if(requestId===sermonRequest)element("SermonResults").replaceChildren(node("p",error.message));}
    }
    function openSermon(id){return visibleStudy(()=>fetchSermon(id));}
    async function fetchSermon(id) {
        const requestId=++sermonRequest;
        sermonListScroll=element("Study").scrollTop;
        element("SermonResults").hidden=true;element("SermonReader").hidden=false;
        element("SermonTitle").textContent="Preparing your sermon…";element("SermonMetadata").textContent="";
        element("SermonContent").replaceChildren(loadingNotice("Shaping the message…", "Opening your selected sermon outline."));
        try {
            const data=await api(apiBase+"/sermons/"+encodeURIComponent(id));
            if(requestId!==sermonRequest)return;
            const sermon=data.sermon;element("SermonTitle").textContent=sermonDisplayTitle(sermon.title);
            const passages=(sermon.ranges||[]).map(span=>books[span.book_id-1].name+" "+(span.chapter_only
                ? span.chapter+(span.end_chapter!==span.chapter?"–"+span.end_chapter:"")
                : span.chapter+":"+span.verse+(span.end_chapter!==span.chapter?"–"+span.end_chapter+":"+span.end_verse:span.end_verse!==span.verse?"–"+span.end_verse:"")));
            const metadata=element("SermonMetadata");metadata.replaceChildren();
            if(sermon.author){
                const prefix=sermon.author.startsWith("Pastor ")?"Pastor ":"";
                if(prefix)metadata.append(document.createTextNode(prefix));
                metadata.append(node("strong",sermon.author.slice(prefix.length)));
            }
            const details=[sermon.source_title,sermon.publisher,sermon.purpose,passages.join("; "),sermon.source_section != null ? "Source page/section "+sermon.source_section : ""].filter(Boolean).join(" · ");
            if(details)metadata.append(document.createTextNode((sermon.author?" · ":"")+details));
            element("SermonContent").replaceChildren();appendSourceBlocks(element("SermonContent"),sermon.blocks);
            notesWholeButton(element("SermonReader"),()=>[...notesPlainBlocks(sermon.title,"heading"),...notesSourceBlocks(sermon.blocks)]);
            element("Study").scrollTop=0;
        } catch(error){if(requestId===sermonRequest){element("SermonTitle").textContent=error.message;element("SermonContent").replaceChildren();}}
    }
    element("SermonBack").addEventListener("click",()=>{
        ++sermonRequest;element("SermonReader").hidden=true;element("SermonResults").hidden=false;element("Study").scrollTop=sermonListScroll;
    });

    function loadCommentary(){return visibleStudy(fetchCommentary);}
    function setIllustrationPassage(verse,end=verse){
        const next={verse,end_verse:end};
        if(JSON.stringify(next)===JSON.stringify(illustrationPassage))return;
        illustrationPassage=next;illustrationSuggestionKey="";
        if(studyTab==="illustrations" && illustrationMode==="suggested")loadIllustrations();
    }
    function illustrationReaderClosed(){
        illustrationArticle=null;element("IllustrationsReader").hidden=true;element("IllustrationsDiscovery").hidden=false;
    }
    function illustrationCard(entry){
        const card=node("article",undefined,"bs-illustration-card");card.dataset.illustrationId=entry.id;
        card.append(node("h4",entry.title),node("p",[entry.topic?.toLocaleUpperCase(),entry.source_title].filter(Boolean).join(" · "),"bs-attribution"));
        if(entry.needs_source_review)card.append(node("p","Content needs source review.","bs-notice"));
        else card.append(node("p",entry.preview));
        if(entry.match){
            const match=entry.match;
            card.append(node("p",(match.link_type==="original"?"Source-provided reference: ":"Suggested topic connection: ")+match.reference+(match.theme?" · "+match.theme:""),"bs-illustration-match"));
        }
        const read=node("button","Read Full Illustration");read.type="button";read.addEventListener("click",()=>openIllustration(entry.id));card.append(read);
        return card;
    }
    function loadIllustrations(){return visibleStudy(()=>illustrationMode==="suggested"?fetchIllustrationSuggestions():fetchIllustrationBrowse());}
    async function fetchIllustrationSuggestions(){
        const params=new URLSearchParams({book:state.book,chapter:state.chapter,...illustrationPassage});
        const key=params.toString();
        if(illustrationSuggestionKey===key){illustrationReaderClosed();return;}
        const requestId=++illustrationRequest,results=element("IllustrationsSuggestions");
        illustrationReaderClosed();results.replaceChildren(loadingNotice("Bringing the message to life…", "Finding illustrations connected to your passage."));
        element("IllustrationsReference").textContent=state.book+" "+state.chapter+(illustrationPassage?":"+illustrationPassage.verse+(illustrationPassage.end_verse!==illustrationPassage.verse?"–"+illustrationPassage.end_verse:""):"");
        try{
            const data=await api(apiBase+"/illustrations/suggested?"+params);
            if(requestId!==illustrationRequest || studyTab!=="illustrations" || illustrationMode!=="suggested")return;
            results.replaceChildren();element("IllustrationsReference").textContent=data.reference;
            if(!data.available){results.append(node("p","The illustration library has not been imported yet. Use Sync Bible Study in reading settings."));return;}
            illustrationSuggestionKey=key;
            if(!data.entries.length){results.append(node("p","No reliable illustration match was found for this passage. Browse by Topic to explore the library."));return;}
            const entries=data.entries.slice(0,30);
            for(const entry of entries.slice(0,10))results.append(illustrationCard(entry));
            if(entries.length>10){
                const more=node("button","See More","bs-illustrations-more");more.type="button";
                more.addEventListener("click",()=>{for(const entry of entries.slice(10))results.insertBefore(illustrationCard(entry),more);more.remove();});results.append(more);
            }
        }catch(error){if(requestId===illustrationRequest)results.replaceChildren(node("p",error.message));}
    }
    async function fetchIllustrationBrowse(offset=0){
        const requestId=++illustrationRequest,results=element("IllustrationsBrowseResults"),topics=element("IllustrationsTopics");
        illustrationReaderClosed();
        const params=new URLSearchParams({letter:illustrationLetter,q:illustrationQuery,topic:illustrationTopic,source:illustrationSource,offset});
        if(!offset){results.replaceChildren(loadingNotice("Bringing the message to life…", "Opening stories and illustrations from your library."));topics.replaceChildren();}
        try{
            const data=await api(apiBase+"/illustrations/browse?"+params);
            if(requestId!==illustrationRequest || studyTab!=="illustrations" || illustrationMode!=="browse")return;
            if(!offset){
                results.replaceChildren();element("IllustrationsTopicsBack").hidden=!illustrationTopic;
                element("IllustrationsTopicHeading").textContent=illustrationTopic?element("IllustrationsTopicHeading").dataset.topicLabel:illustrationQuery?"Search results":"Topics · "+illustrationLetter;
                topics.hidden=Boolean(illustrationTopic);
                if(!data.available){results.append(node("p","The illustration library has not been imported yet. Use Sync Bible Study in reading settings."));return;}
                if(!illustrationTopic)for(const topic of data.topics){
                    const label=topic.display_name.toLocaleUpperCase(),button=node("button",label+" ("+topic.illustration_count+")");button.type="button";
                    button.addEventListener("click",()=>{illustrationTopic=topic.topic_key;illustrationQuery="";element("IllustrationsSearch").value="";element("IllustrationsTopicHeading").dataset.topicLabel=label;loadIllustrations();});topics.append(button);
                }
                if(!illustrationTopic&&!illustrationQuery&&!data.topics.length)topics.append(node("p","No topics for this letter and collection."));
                if((illustrationTopic||illustrationQuery)&&!data.entries.length)results.append(node("p","No illustrations match this search."));
            }
            for(const entry of data.entries)results.append(illustrationCard(entry));
            if(data.next_offset!==null){
                const more=node("button","More illustrations");more.type="button";
                more.addEventListener("click",async()=>{more.disabled=true;await visibleStudy(()=>fetchIllustrationBrowse(data.next_offset));more.remove();});results.append(more);
            }
        }catch(error){if(requestId===illustrationRequest){if(!offset)results.replaceChildren();results.append(node("p",error.message));}}
    }
    async function openIllustration(id){
        const requestId=++illustrationRequest;illustrationListScroll=element("Study").scrollTop;
        element("IllustrationsDiscovery").hidden=true;element("IllustrationsReader").hidden=false;
        element("IllustrationsTitle").textContent="Preparing your illustration…";element("IllustrationsMetadata").textContent="";
        element("IllustrationsContent").replaceChildren(loadingNotice("Bringing the message to life…", "Opening your selected illustration."));element("IllustrationsReferences").replaceChildren();element("IllustrationsCredit").textContent="";element("IllustrationsCopy").disabled=true;
        illustrationArticle=null;
        try{
            const data=await visibleStudy(()=>api(apiBase+"/illustrations/entry/"+encodeURIComponent(id)));
            if(requestId!==illustrationRequest || studyTab!=="illustrations")return;
            const article=illustrationArticle=data.illustration,content=element("IllustrationsContent");
            content.replaceChildren();
            element("IllustrationsTitle").textContent=article.title;
            element("IllustrationsMetadata").textContent=[article.source_title,...(article.topics.length?article.topics:[article.topic]).map(topic=>topic?.toLocaleUpperCase())].filter(Boolean).join(" · ");
            appendSourceBlocks(content,article.blocks);
            if(article.needs_source_review)content.append(node("p","Content needs source review. This record has no stored illustration body.","bs-notice"));
            const credit=element("IllustrationsCredit");
            if(article.source_credit&&!article.credit_in_body)credit.append(document.createTextNode(article.source_credit+" · "));
            credit.append(document.createTextNode("Source: "+article.source_title));
            if(article.source_file){
                credit.append(document.createTextNode(" · "));
                let sourceUrl=null;try{sourceUrl=new URL(article.source_file);}catch(_){}
                if(sourceUrl&&["https:","http:"].includes(sourceUrl.protocol)){
                    const link=node("a","Original source");link.href=sourceUrl.href;link.target="_blank";link.rel="noopener noreferrer";credit.append(link);
                }else credit.append(document.createTextNode(article.source_file));
            }
            const references=element("IllustrationsReferences");
            for(const [kind,label] of [["original","Original Bible references"],["suggested","Suggested related Scripture"]]){
                const section=node("section",undefined,"bs-illustration-references");section.append(node("h4",label));
                const links=article.references.filter(ref=>ref.link_type===kind);
                if(kind==="suggested")section.append(node("p","These topic connections are provided for you to assess against the passage.","bs-attribution"));
                if(!links.length)section.append(node("p",kind==="original"?"No usable source-provided references recorded.":"No suggested references recorded."));
                for(const ref of links){
                    const line=node("p"),button=node("button",ref.display_reference,"bs-scripture-ref");button.type="button";
                    button.addEventListener("click",()=>{const book=books[ref.book_id-1].name;if(ref.verse_start===null)navigatePassage(book,ref.chapter_start,1,true);else previewVerse({book,chapter:ref.chapter_start,verse:ref.verse_start,end_chapter:ref.chapter_end,end_verse:ref.verse_end});});line.append(button);
                    if(kind==="suggested"&&ref.theme)line.append(document.createTextNode(" · "+ref.theme));section.append(line);
                }
                references.append(section);
            }
            if(article.reference_issues.length){const note=node("details");note.append(node("summary","Source references needing review"));for(const issue of article.reference_issues)note.append(node("p",issue.display_reference+" · "+issue.reason));references.append(note);}
            element("IllustrationsCopy").disabled=article.needs_source_review;
            element("IllustrationsTitle").focus({preventScroll:true});element("Study").scrollTop=Math.max(0,element("Study").scrollTop+element("IllustrationsReader").getBoundingClientRect().top-element("Study").getBoundingClientRect().top-60);
        }catch(error){if(requestId===illustrationRequest){element("IllustrationsTitle").textContent=error.message;element("IllustrationsContent").replaceChildren();}}
    }
    element("IllustrationsBack").addEventListener("click",()=>{++illustrationRequest;illustrationReaderClosed();element("Study").scrollTop=illustrationListScroll;});
    element("IllustrationsCopy").addEventListener("click",()=>{if(illustrationArticle){const a=illustrationArticle;copyScripture([a.title,a.body_text,a.credit_in_body?"":a.source_credit,a.source_title,a.source_file].filter(Boolean).join("\n\n"),"Illustration");}});
    notesWholeButton(element("IllustrationsReader"),()=>illustrationArticle?
        [...notesPlainBlocks(illustrationArticle.title,"heading"),...notesBlocksFromDOM(element("IllustrationsContent")),
         ...notesPlainBlocks(illustrationArticle.source_credit||illustrationArticle.source_title||"")]:[]);
    function selectIllustrationMode(mode,focus=false){
        illustrationMode=mode;++illustrationRequest;illustrationReaderClosed();
        for(const button of root.querySelectorAll("[data-illustration-mode]")){const active=button.dataset.illustrationMode===mode;button.setAttribute("aria-selected",String(active));button.tabIndex=active?0:-1;if(active&&focus)button.focus();}
        element("IllustrationsSuggested").hidden=mode!=="suggested";element("IllustrationsBrowse").hidden=mode!=="browse";loadIllustrations();
    }
    root.querySelectorAll("[data-illustration-mode]").forEach(button=>{
        button.addEventListener("click",()=>selectIllustrationMode(button.dataset.illustrationMode));
        button.addEventListener("keydown",event=>{if(["ArrowLeft","ArrowRight","Home","End"].includes(event.key)){event.preventDefault();selectIllustrationMode(event.key==="Home"?"suggested":event.key==="End"?"browse":illustrationMode==="suggested"?"browse":"suggested",true);}});
    });
    for(const letter of "ABCDEFGHIJKLMNOPQRSTUVWXYZ"){
        const button=node("button",letter);button.type="button";button.setAttribute("aria-pressed",String(letter===illustrationLetter));
        button.addEventListener("click",()=>{illustrationLetter=letter;illustrationTopic="";illustrationQuery="";element("IllustrationsSearch").value="";for(const other of element("IllustrationsAlphabet").children)other.setAttribute("aria-pressed",String(other===button));loadIllustrations();});element("IllustrationsAlphabet").append(button);
    }
    element("IllustrationsSearchForm").addEventListener("submit",event=>{event.preventDefault();illustrationQuery=element("IllustrationsSearch").value.trim();illustrationTopic="";loadIllustrations();});
    element("IllustrationsSource").addEventListener("change",event=>{illustrationSource=event.target.value;loadIllustrations();});
    element("IllustrationsTopicsBack").addEventListener("click",()=>{illustrationTopic="";loadIllustrations();});

    function clearCommentaryVerseFocus(){
        commentaryVerseTarget=null;
        element("CommentaryResults").querySelectorAll(".bs-commentary-verse-target").forEach(block=>block.classList.remove("bs-commentary-verse-target"));
    }
    function requestCommentaryVerse(verse){
        clearCommentaryVerseFocus();
        if(studyTab==="commentary"&&commentaryFullChapter()){
            // Resolve an owner change before recording the user's new target.
            commentaryQuery();
            commentaryVerseTarget={owner:commentaryThemeOwner,bookId:books.findIndex(book=>book.name===state.book)+1,chapter:state.chapter,verse};
        }
    }
    function focusCommentaryVerse(){
        const target=commentaryVerseTarget;if(!target)return;
        clearCommentaryVerseFocus();
        if(studyTab!=="commentary"||!commentaryFullChapter()||target.owner!==commentaryThemeOwner)return;
        const point=target.chapter*200+target.verse,matches=[];
        for(const part of commentaryVerseParts)for(const span of part.ranges){
            const start=span.chapter*200+span.verse,end=span.end_chapter*200+span.end_verse;
            if(span.book_id===target.bookId&&start<=point&&point<=end)matches.push({part,width:end-start});
        }
        // A verse explanation takes precedence over a broad chapter introduction.
        matches.sort((a,b)=>a.width-b.width);const part=matches[0]?.part;
        if(!part?.nodes.length)return;
        for(const block of part.nodes)block.classList.add("bs-commentary-verse-target");
        const pane=element("Study"),tabs=mobile.matches?studyTabs.getBoundingClientRect().height:0;
        pane.scrollTop+=part.nodes[0].getBoundingClientRect().top-pane.getBoundingClientRect().top-tabs-element("CommentaryControls").getBoundingClientRect().height-10;
    }
    element("CommentaryPanel").addEventListener("click",clearCommentaryVerseFocus,true);
    element("CommentaryTab").addEventListener("click",clearCommentaryVerseFocus);
    function selectCommentarySection(id){
        if(studyTab!=="commentary"||commentarySectionsOwner!==commentaryThemeOwner||!commentarySections.some(theme=>theme.id===id))return;
        element("CommentarySectionsDialog").close();
        if(id===commentarySelectedSection&&commentaryRenderedKey)return;
        commentaryTheme=id;commentaryScrollToTheme=id;loadCommentary();
    }
    function commentaryNavigationBusy(busy){
        const index=commentarySections.findIndex(theme=>theme.id===commentarySelectedSection);
        element("CommentaryPrevious").disabled=busy||index<=0;
        element("CommentaryNext").disabled=busy||index<0||index>=commentarySections.length-1;
    }
    element("CommentaryViewAll").addEventListener("click",()=>{
        if(commentarySectionsOwner!==commentaryThemeOwner||commentarySections.length<2)return;
        const dialog=element("CommentarySectionsDialog");dialog.showModal();
        element("CommentarySectionsList").querySelector('[aria-pressed="true"]')?.focus({preventScroll:true});
    });
    element("CloseCommentarySections").addEventListener("click",()=>element("CommentarySectionsDialog").close());
    element("CommentarySectionsDialog").addEventListener("click",event=>{
        if(event.target!==event.currentTarget)return;
        const bounds=event.currentTarget.getBoundingClientRect();
        if(event.clientX<bounds.left||event.clientX>bounds.right||event.clientY<bounds.top||event.clientY>bounds.bottom)event.currentTarget.close();
    });
    for(const [name,step] of [["CommentaryPrevious",-1],["CommentaryNext",1]])element(name).addEventListener("click",()=>{
        const index=commentarySections.findIndex(theme=>theme.id===commentarySelectedSection),next=commentarySections[index+step];
        if(index>=0&&next)selectCommentarySection(next.id);
    });
    function renderCommentaryThemes(data,owner){
        const box=element("CommentaryThemes"),list=element("CommentaryThemeList"),themes=data.themes||[];
        const scroll=list.scrollLeft,focused=list.contains(document.activeElement);list.replaceChildren();box.dataset.owner=owner;
        const fullChapter=commentaryFullChapter(),hasChoices=!fullChapter&&themes.length>1;
        commentarySections=fullChapter?[]:themes;commentarySelectedSection=data.selected_theme||"";commentarySectionsOwner=owner;
        box.hidden=!hasChoices;
        element("CommentaryGuide").hidden=!fullChapter;
        element("CommentaryGuide").textContent="Full chapter commentary.";
        element("CommentaryThemesTitle").textContent=themes.length+" commentary sections in this chapter";
        element("CommentaryViewAll").textContent="View all ("+themes.length+")";
        const selectedIndex=themes.findIndex(theme=>theme.id===data.selected_theme);
        element("CommentarySectionNav").hidden=!hasChoices||selectedIndex<0;
        element("CommentarySectionPosition").textContent=selectedIndex>=0?"Section "+(selectedIndex+1)+" of "+themes.length:"";
        commentaryNavigationBusy(false);
        element("CommentarySectionsReference").textContent=commentaryNames[commentarySeries]+" · "+state.book+" "+state.chapter;
        const all=element("CommentarySectionsList");all.replaceChildren();
        for(const [index,theme] of themes.entries()){
            const button=node("button",undefined,"bs-commentary-theme");button.type="button";button.dataset.commentaryTheme=theme.id;
            button.setAttribute("aria-pressed",String(theme.id===data.selected_theme));button.title=[theme.title,theme.passage,theme.source_title].filter(Boolean).join(" · ");
            button.append(node("span",(index+1)+". "+theme.title),node("small",theme.passage));
            button.addEventListener("click",()=>selectCommentarySection(theme.id));list.append(button);
            const choice=node("button",undefined,"bs-commentary-section-choice");choice.type="button";
            choice.dataset.commentaryTheme=theme.id;choice.setAttribute("aria-pressed",String(theme.id===data.selected_theme));
            const number=node("span",index+1,"bs-commentary-section-number");number.setAttribute("aria-hidden","true");
            const label=node("span",undefined,"bs-commentary-section-label");label.append(node("strong",theme.title),node("small",theme.passage));
            const check=node("span",theme.id===data.selected_theme?"✓":"","bs-commentary-section-check");check.setAttribute("aria-hidden","true");
            choice.append(number,label,check);choice.addEventListener("click",()=>selectCommentarySection(theme.id));all.append(choice);
        }
        list.scrollLeft=scroll;
        const active=list.querySelector('[aria-pressed="true"]');
        if(active){const bounds=active.getBoundingClientRect(),viewport=list.getBoundingClientRect();if(bounds.left<viewport.left||bounds.right>viewport.right)list.scrollLeft+=bounds.left-viewport.left;}
        if(focused)list.querySelector('[aria-pressed="true"]')?.focus({preventScroll:true});
    }
    async function fetchCommentary() {
        const query=commentaryQuery(),owner=commentaryThemeOwner,renderedKey=query.toString();
        if(commentaryRenderedKey===renderedKey && Date.now()-commentaryRenderedAt<sourceCacheLifetime){focusCommentaryVerse();return;}
        commentaryRenderedKey="";
        const requestId = ++commentaryRequest;
        const results = element("CommentaryResults");
        commentaryVerseParts=[];
        commentaryNavigationBusy(true);
        if(element("CommentaryThemes").dataset.owner!==owner){
            commentarySections=[];commentarySelectedSection="";commentarySectionsOwner="";
            element("CommentaryThemes").hidden=true;element("CommentaryThemeList").replaceChildren();
            element("CommentarySectionNav").hidden=true;element("CommentarySectionsList").replaceChildren();element("CommentarySectionsDialog").close();
        }
        results.replaceChildren(loadingNotice("Gathering wisdom…", "Preparing " + commentaryNames[commentarySeries] + " for " + state.book + " " + state.chapter + "."));
        try {
            await chapterLoadPromise;
            if(requestId!==commentaryRequest || studyTab!=="commentary")return;
            const data = await api(apiBase + "/commentary?" + query);
            if (requestId !== commentaryRequest || studyTab !== "commentary") return;
            renderCommentaryThemes(data,owner);
            results.replaceChildren(node("h3", data.reference));
            if (!data.available) results.append(node("p", commentaryNames[commentarySeries] + " is not available in the study library yet."));
            else if (!data.entries.length) results.append(node("p", "No indexed " + commentaryNames[commentarySeries] + " section was found for this chapter."));
            for (const entry of data.entries) {
                const article = node("article", undefined, "bs-commentary-entry");
                article.append(node("p", entry.source_title, "bs-commentary-volume"), node("h4", entry.title),
                    node("p", "Passage covered: " + entry.passage, "bs-commentary-range"));
                const content = node("div", undefined, "bs-source-content bs-commentary-text");
                if (["moody","pulpit"].includes(commentarySeries)) content.classList.add("bs-source-lines");
                let outline = null;const renderedBlocks=[];
                for (const block of entry.blocks) {
                    if (commentarySeries === "exalting-jesus" && ["commentary_outline", "commentary_outline_continuation"].includes(block.role)) {
                        if (!outline) {
                            outline = node("section", undefined, "bs-commentary-outline-card");
                            outline.setAttribute("aria-label", "Commentary outline");
                            outline.append(node("h5", "Outline")); content.append(outline);
                        }
                        outline.append(renderSourceBlock(block));
                    } else { outline = null;const rendered=renderSourceBlock(block,entry.blocks);content.append(rendered);renderedBlocks.push(rendered); }
                }
                if(commentaryFullChapter())for(const part of entry.verse_sections||[]){
                    if(Array.isArray(part.ranges)&&Number.isInteger(part.start_block)&&Number.isInteger(part.end_block)&&part.start_block>=0&&part.end_block<=renderedBlocks.length)
                        commentaryVerseParts.push({ranges:part.ranges,nodes:renderedBlocks.slice(part.start_block,part.end_block)});
                }
                article.append(content, node("p", entry.author + " · " + entry.source_title, "bs-attribution"));
                notesWholeButton(article,()=>[...notesPlainBlocks(entry.title,"heading"),...notesSourceBlocks(entry.blocks),
                    ...notesPlainBlocks([entry.author,entry.source_title].filter(Boolean).join(" · "))]);
                results.append(article);
            }
            commentaryRenderedKey=renderedKey;commentaryRenderedAt=Date.now();
            focusCommentaryVerse();
            if(commentaryScrollToTheme&&commentaryScrollToTheme===data.selected_theme){
                const pane=element("Study"),tabs=mobile.matches?studyTabs.getBoundingClientRect().height:0;
                pane.scrollTop+=results.getBoundingClientRect().top-pane.getBoundingClientRect().top-tabs-element("CommentaryControls").getBoundingClientRect().height-8;
                commentaryScrollToTheme="";
            }
        } catch (error) {
            if (requestId === commentaryRequest){results.replaceChildren(node("p", error.message));commentaryNavigationBusy(false);}
        }
    }

    root.querySelectorAll("[data-commentary-series]").forEach(button => button.addEventListener("click", () => {
        const series = button.dataset.commentarySeries;
        if (!commentaryNames[series] || series === commentarySeries) return;
        commentarySeries = series;
        element("PulpitSections").hidden=series!=="pulpit";
        root.querySelectorAll("[data-commentary-series]").forEach(item => item.setAttribute("aria-pressed", String(item === button)));
        if (studyTab === "commentary") loadCommentary();
    }));

    root.querySelectorAll("[data-pulpit-section]").forEach(button=>button.addEventListener("click",()=>{
        pulpitSection=button.dataset.pulpitSection;
        root.querySelectorAll("[data-pulpit-section]").forEach(item=>item.setAttribute("aria-pressed",String(item===button)));
        if(studyTab==="commentary" && commentarySeries==="pulpit")loadCommentary();
    }));

    function loadConcordance(query,offset=0,ownerRequest=null){return visibleStudy(()=>fetchConcordance(query,offset,ownerRequest));}
    async function fetchConcordance(query, offset = 0, ownerRequest = null) {
        const requestId = offset === 0 ? ++concordanceRequest : ownerRequest;
        const results = element("ConcordanceResults");
        if (offset === 0) results.replaceChildren(loadingNotice("Connecting the Scriptures…", "Finding passages and word matches for “" + query + "”."));
        try {
            const data = await api(apiBase + "/concordance?" + new URLSearchParams({q: query, offset, translation: state.translation}));
            if (requestId !== concordanceRequest) return;
            if (offset === 0) results.replaceChildren(node("h3", data.query));
            if (!data.available) { results.append(node("p", state.translation === "NIV" ? "The Thompson concordance has not been synced yet." : "This translation has not been imported yet.")); return; }
            if (!data.entries.length && offset === 0) results.append(node("p", state.translation === "NIV" ? "No matching entry found in Thompson." : "No verses contain this exact word in the selected translation."));
            for (const entry of data.entries) {
                const card = node("article", undefined, "bs-concordance-entry");
                card.append(node("h4", entry.headword));
                if (entry.related?.length) {
                    const related = node("div", undefined, "bs-concordance-related");
                    related.append(node("span", "Related forms: "));
                    for (const word of entry.related) {
                        const button = node("button", word, "bs-concordance-form"); button.type = "button";
                        button.addEventListener("click", () => {
                            selectedWord = {word}; element("Search").value = element("ConcordanceSearch").value = word;
                            loadConcordance(word);
                        }); related.append(button);
                    }
                    card.append(related);
                }
                const content = node("div", undefined, "bs-concordance-excerpts bs-source-content");
                for (const block of entry.blocks) content.append(renderSourceBlock(block));
                card.append(content, node("p", entry.source_label || "Thompson concordance · PDF page " + entry.source_page, "bs-attribution"));
                if(entry.topic_id){
                    const open=node("button","Open chain topic");open.type="button";
                    open.addEventListener("click",()=>previewChain(entry.topic_id));card.append(open);
                }
                if (entry.passage) {
                    const read = node("button", "Read passage"); read.type = "button";
                    read.addEventListener("click", () => navigatePassage(entry.passage.book, entry.passage.chapter, entry.passage.verse));
                    card.append(read);
                }
                results.append(card);
            }
            if (data.next_offset !== null) {
                const more = node("button", "More entries"); more.type = "button";
                more.addEventListener("click", async () => {
                    more.disabled = true; await loadConcordance(query, data.next_offset, requestId); more.remove();
                }); results.append(more);
            }
        } catch (error) {
            if (requestId === concordanceRequest) results.replaceChildren(node("p", error.message));
        }
    }
    element("ConcordanceForm").addEventListener("submit", event => {
        event.preventDefault(); const query = element("ConcordanceSearch").value.trim();
        if (!query) return;
        selectedWord = {word: query}; element("Search").value = query; loadConcordance(query);
        ++wordRequest;
    });

    function sourceSection(section) {
        const details = node("details", undefined, "bs-study-section");
        details.append(node("summary", section.title));
        const content = node("div", undefined, "bs-source-content");
        for (const block of section.blocks || []) content.append(renderSourceBlock(block));
        if (section.pages?.length) {
            const pages = node("div", undefined, "bs-source-pages");
            for (const number of section.pages) {
                const button = node("button", "View page " + number); button.type = "button";
                button.addEventListener("click", () => {
                    element("SourceTitle").textContent = section.title + " · PDF page " + number;
                    element("SourceImage").src = apiBase + "/studies/page/" + number;
                    element("SourceImage").alt = section.title + ", original Thompson PDF page " + number;
                    if (!element("SourceDialog").open) element("SourceDialog").showModal();
                }); pages.append(button);
            }
            content.append(pages);
        }
        content.append(node("p", "Thompson Chain-Reference Bible · PDF page " + section.source_page, "bs-attribution"));
        details.append(content); return details;
    }

    function renderPeople(data, ownerRequest, target=element("WordResults")) {
        if (!data?.people?.length) return;
        const card = node("section", undefined, "bs-person-study");
        card.append(node("p", "Thompson person study", "bs-kicker"));
        const body = node("div"); card.append(body); let personSelection = 0;
        function show(person) {
            body.replaceChildren(node("h4", person.label));
            const preview = person.sections.flatMap(s => s.blocks || []).find(b => b.type !== "heading");
            if (preview) {
                const characters = Array.from(preview.text);
                const text = characters.length > 320 ? characters.slice(0, 320).join("").replace(/\s+\S*$/, "") + "…" : preview.text;
                body.append(renderSourceBlock({type: "paragraph", text, references: (preview.references || []).filter(ref => ref.end <= Array.from(text).length)}));
            }
            for (const section of person.sections) body.append(sourceSection(section));
            if (person.topics?.length) {
                const topics = node("div", undefined, "bs-source-pages");
                for (const topic of person.topics) {
                    const button = node("button", topic.topic_key + " · " + topic.title); button.type = "button";
                    button.addEventListener("click", () => previewChain(topic.id)); topics.append(button);
                }
                body.append(topics);
            }
        }
        if (data.people.length > 1 || !data.selected) {
            card.append(node("p", "Choose the person you want to study."));
            for (const person of data.people) {
                const button = node("button", person.label, "bs-person-choice"); button.type = "button";
                button.addEventListener("click", async () => {
                    const selection = ++personSelection; button.disabled = true;
                    try {
                        const result = await api(apiBase + "/people/" + person.id);
                        if (ownerRequest !== wordRequest || selection !== personSelection) return;
                        show(result.person);
                    } catch (error) { if (ownerRequest === wordRequest && selection === personSelection) body.replaceChildren(node("p", error.message)); }
                    finally { button.disabled = false; }
                }); card.append(button);
            }
        }
        if (data.selected) show(data.selected);
        target.insertBefore(card, target.children[1] || null);
    }

    function loadBookStudy(){return visibleStudy(fetchBookStudy);}
    async function fetchBookStudy() {
        if (bookStudyBook === state.book) return;
        const requestId = ++bookStudyRequest, book = state.book;
        element("BookStudyTitle").textContent = book;
        element("BookStudyResults").replaceChildren(loadingNotice("Discovering the bigger picture…", "Preparing your introduction to " + book + "."));
        try {
            await chapterLoadPromise;
            if(requestId!==bookStudyRequest || state.book!==book)return;
            const data = await api(apiBase + "/book-study?" + new URLSearchParams({book}));
            if (requestId !== bookStudyRequest || state.book !== book) return;
            if (data.available) bookStudyBook = book;
            const content = node("div", undefined, "bs-source-content");
            if (!data.available) content.append(node("p", "Thompson book studies have not been synced yet."));
            else {
                for (const block of data.study.blocks) content.append(renderSourceBlock(block));
                if (data.study.outline?.blocks?.length) {
                    const outline = node("section", undefined, "bs-condensed-outline");
                    outline.append(node("h4", "Condensed Outline of the Bible"));
                    for (const block of data.study.outline.blocks) outline.append(renderSourceBlock(block));
                    content.append(outline);
                }
                content.append(node("p", "Thompson book introduction · PDF page " + data.study.source_page, "bs-attribution"));
            }
            element("BookStudyResults").replaceChildren(content);
        } catch (error) { if (requestId === bookStudyRequest) element("BookStudyResults").replaceChildren(node("p", error.message)); }
    }
    // Personal sermon documents share the study workspace, but have their own
    // save queue and owner-scoped recovery drafts, independent of reader state.
    let notesLoaded=false,notesItems=[],notesActive=null,notesChanged=false,notesSerial=0,notesOpenRequest=0;
    let notesSaveChain=Promise.resolve(),notesTimer=null,notesRange=null,notesEditingMeta=false;
    let notesPendingBlocks=[],notesReadMode=false,notesJobTimer=null,notesPDF=null,notesPDFUrl=null;
    let notesLibraryView="recent",notesLibraryBook=0;
    let notesInlineReferences=[],notesReferenceTimer=null,notesReferencePointer=null,notesComposing=false,notesReferencePattern=null;
    const notesEditor=element("NotesEditor"),notesDraftKey=preferenceKey+"-sermon-draft-";
    const notesFonts=[...element("NotesFont").options].map(option=>option.value);
    const notesActions=element("NotesActionsMenu"),notesActionsToggle=element("NotesActionsToggle");
    let notesActionsOpen=false,notesActionsPointerUntil=0;
    if(typeof notesActions.showPopover==="function")notesActions.setAttribute("popover","auto");
    function notesPositionActions(){
        if(!notesActionsOpen)return;
        const viewport=window.visualViewport,left=viewport?.offsetLeft||0,top=viewport?.offsetTop||0;
        const width=viewport?.width||innerWidth,height=viewport?.height||innerHeight,anchor=notesActionsToggle.getBoundingClientRect();
        notesActions.style.setProperty("--bs-notes-options-height",Math.max(100,height-16)+"px");
        const bounds=notesActions.getBoundingClientRect(),x=Math.max(left+8,Math.min(anchor.right-bounds.width,left+width-bounds.width-8));
        const below=anchor.bottom+6,y=below+bounds.height<=top+height-8?below:Math.max(top+8,anchor.top-bounds.height-6);
        notesActions.style.setProperty("--bs-notes-options-left",x+"px");notesActions.style.setProperty("--bs-notes-options-top",y+"px");
    }
    function notesCloseActions(focusToggle=false){
        notesActionsOpen=false;notesActionsPointerUntil=0;notesActionsToggle.setAttribute("aria-expanded","false");
        if(notesActions.hasAttribute("popover")&&notesActions.matches(":popover-open"))notesActions.hidePopover();
        notesActions.hidden=true;
        if(focusToggle&&notesActionsToggle.getClientRects().length)notesActionsToggle.focus();
    }
    notesActionsToggle.addEventListener("click",()=>{
        if(notesActionsOpen){notesCloseActions();return;}
        notesCloseMenu();notesActionsOpen=true;notesActions.hidden=false;notesActionsToggle.setAttribute("aria-expanded","true");
        if(notesActions.hasAttribute("popover"))notesActions.showPopover();
        notesPositionActions();notesActions.querySelector("button:not([disabled]):not([hidden])")?.focus({preventScroll:true});
    });
    notesActions.addEventListener("toggle",event=>{if(event.newState==="closed")notesCloseActions();});
    // Safari moves focus back to the reading pane during a touch-generated
    // mousedown. Keep the menu alive until the tapped button receives its click.
    notesActions.addEventListener("pointerdown",()=>{notesActionsPointerUntil=performance.now()+800;},{passive:true});
    notesActions.addEventListener("click",event=>{if(event.target.closest("button:not([disabled])"))notesCloseActions();});
    document.addEventListener("pointerdown",event=>{
        if(notesActionsOpen&&!notesActions.contains(event.target)&&!notesActionsToggle.contains(event.target))notesCloseActions();
    },true);
    document.addEventListener("keydown",event=>{
        if(event.key==="Tab")notesActionsPointerUntil=0;
        if(event.key==="Escape"&&notesActionsOpen){event.preventDefault();event.stopPropagation();notesCloseActions(true);}
    },true);
    notesActions.addEventListener("focusout",event=>{
        if(performance.now()>=notesActionsPointerUntil&&event.relatedTarget&&!notesActions.contains(event.relatedTarget)&&event.relatedTarget!==notesActionsToggle)notesCloseActions();
    });
    element("Bible").addEventListener("scroll",()=>{if(notesActionsOpen)notesCloseActions();},{passive:true});
    window.addEventListener("resize",notesPositionActions);window.visualViewport?.addEventListener("resize",notesPositionActions);
    const notesTools=element("NotesTools");
    let notesMenuOpen=false,notesToolbarPointerUntil=0;
    root.dataset.notesMenu="selection";
    const notesMainActions=notesTools.closest(".bs-notes-dock").querySelector(".bs-notes-actions");
    const notesOriginalInputMode=notesEditor.getAttribute("inputmode");
    function notesFormattingKeyboard(active){
        if(active){notesEditor.setAttribute("inputmode","none");notesEditor.setAttribute("virtualkeyboardpolicy","manual");}
        else {if(notesOriginalInputMode===null)notesEditor.removeAttribute("inputmode");else notesEditor.setAttribute("inputmode",notesOriginalInputMode);notesEditor.removeAttribute("virtualkeyboardpolicy");}
        try {if(active)navigator.virtualKeyboard?.hide();window.IOWOAndroid?.setNotesFormatting?.(active);}catch(_){}
    }
    function notesPaintFormattingSelection(){
        if(!window.CSS?.highlights||typeof window.Highlight!=="function")return;
        if(notesMenuOpen&&notesRange&&!notesRange.collapsed&&notesEditor.contains(notesRange.commonAncestorContainer))CSS.highlights.set("sermon-format-selection",new Highlight(notesRange));
        else CSS.highlights.delete("sermon-format-selection");
    }
    function notesParkFormattingFocus(preferred){
        if(!notesMenuOpen)return;
        const selection=window.getSelection();
        if(selection?.rangeCount&&notesEditor.contains(selection.anchorNode)&&notesEditor.contains(selection.focusNode))notesRange=selection.getRangeAt(0).cloneRange();
        notesPaintFormattingSelection();
        if(!iosViewport&&!navigator.maxTouchPoints&&!window.matchMedia("(any-pointer: coarse)").matches&&!window.IOWOAndroid)return;
        if(notesEditor===document.activeElement||!notesTools.contains(document.activeElement)){
            const target=preferred&&notesTools.contains(preferred)?preferred:notesTools;
            target.focus({preventScroll:true});
        }
        notesEditor.blur();try {navigator.virtualKeyboard?.hide();}catch(_){}
    }
    function notesDisplaySize(value){
        const control=element("NotesFontSize"),key=String(value);
        if(![...control.options].some(option=>option.value===key))control.add(new Option(key,key));
        control.value=key;
    }
    function notesCloseMenu(){
        notesMenuOpen=false;notesTools.removeAttribute("data-open");
        notesTools.hidden=true;notesMainActions.hidden=false;
        notesFormattingKeyboard(false);notesPaintFormattingSelection();
    }
    function notesShowSelectionTools(){
        if(notesReadMode||element("NotesPanel").hidden||element("NotesWorkspace").hidden)return notesCloseMenu();
        const selection=window.getSelection();
        if(!selection?.rangeCount||selection.isCollapsed||!selection.toString().trim()||!notesEditor.contains(selection.anchorNode)||!notesEditor.contains(selection.focusNode))return;
        notesRange=selection.getRangeAt(0).cloneRange();notesCloseActions();
        if(!notesMenuOpen){
            notesMenuOpen=true;notesMainActions.hidden=true;notesTools.hidden=false;notesTools.dataset.open="true";
            notesFormattingKeyboard(true);notesParkFormattingFocus();
        }
    }
    notesTools.addEventListener("pointerdown",()=>{notesToolbarPointerUntil=performance.now()+1000;},{passive:true});
    // Keep the editor's native range and formatting/save functions.
    notesTools.addEventListener("mousedown",event=>{if(event.target.closest("button"))event.preventDefault();});
    notesTools.addEventListener("click",event=>{
        const button=event.target.closest("[data-notes-selection-command]");if(!button)return;
        const command=button.dataset.notesSelectionCommand;
        if(command==="hiliteColor")element("NotesHighlightColor").value=button.dataset.color;
        notesFormat(command,button.dataset.color);
    });
    element("NotesCopySelection").addEventListener("click",async()=>{
        const text=notesRange?.toString();if(text)await copyScripture(text,"Sermon Notes");
        notesParkFormattingFocus(element("NotesCopySelection"));
    });
    element("NotesDismissSelection").addEventListener("click",notesCloseMenu);
    element("NotesErase").addEventListener("click",()=>{
        if(!notesActive||notesReadMode||!notesRange||notesRange.collapsed||!notesEditor.contains(notesRange.commonAncestorContainer))return;
        notesFormat("delete");
    });
    notesEditor.addEventListener("pointerdown",()=>{if(notesMenuOpen)notesCloseMenu();},true);
    document.addEventListener("pointerdown",event=>{
        if(notesMenuOpen&&!notesTools.contains(event.target)&&!notesEditor.contains(event.target))notesCloseMenu();
    },true);
    document.addEventListener("keydown",event=>{if(event.key==="Escape"&&notesMenuOpen){event.preventDefault();event.stopPropagation();notesCloseMenu();}},true);
    const notesMenuVisibility=new MutationObserver(()=>{
        if(element("NotesPanel").hidden||element("NotesWorkspace").hidden)notesCloseMenu();
        if(element("NotesPanel").hidden)notesCloseActions();
        positionWorkspaceTools();
        if(!element("NotesLibrary").hidden)element("NotesAll").setAttribute("aria-current","page");
        else element("NotesAll").removeAttribute("aria-current");
    });
    for(const name of ["NotesPanel","NotesWorkspace","NotesLibrary"])notesMenuVisibility.observe(element(name),{attributes:true,attributeFilter:["hidden"]});
    function notesStatus(message,error=false){element("NotesStatus").textContent=message;element("NotesStatus").dataset.error=String(error);}
    function notesDetectType(text){
        if(/^\s*[IVXLCDM]+\s*[.\-–—]\s+/.test(text))return "main_point";
        if(/^\s*[A-Z][.)]\s+/.test(text))return "subpoint";
        if(/^\s*(?:\d+|[a-z])[.)]\s+/.test(text))return "sub_subpoint";
        if(/^\s*(illustration|story|example)\s*:/i.test(text))return "illustration";
        if(/^\s*(conclusion|application)\s*:/i.test(text))return "conclusion";
        return "paragraph";
    }
    function notesPlainBlocks(text,type=null){return String(text).split(/\n\s*\n|\n/).map(line=>({type:type||notesDetectType(line),runs:[{text:line}]}));}
    function notesColor(value){
        if(!value||value==="transparent")return null;
        if(/^#[a-f0-9]{6}$/i.test(value))return value;
        const rgb=value.match(/^rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        return rgb?"#"+rgb.slice(1).map(v=>Number(v).toString(16).padStart(2,"0")).join(""):null;
    }
    function notesRuns(container){
        const runs=[];
        function walk(item,inherited={}){
            if(item.nodeType===Node.TEXT_NODE){if(item.textContent)runs.push({...inherited,text:item.textContent});return;}
            if(item.nodeType!==Node.ELEMENT_NODE)return;
            if(["SCRIPT","STYLE","IMG","IFRAME"].includes(item.tagName))return;
            const style={...inherited},css=item.style;
            if(["B","STRONG"].includes(item.tagName))style.bold=true;
            if(["I","EM"].includes(item.tagName))style.italic=true;
            if(item.tagName==="U")style.underline=true;
            if(item.tagName==="SUP"||css.verticalAlign==="super")style.superscript=true;
            if(item.tagName==="MARK")style.highlight="#ffe66d";
            if(css.fontWeight)style.bold=css.fontWeight==="bold"||Number(css.fontWeight)>=600;
            if(css.fontStyle)style.italic=css.fontStyle==="italic";
            if(css.textDecoration)style.underline=css.textDecoration.includes("underline");
            const color=notesColor(css.color||item.getAttribute("color")),highlight=notesColor(css.backgroundColor);
            if(color)style.color=color;if(highlight)style.highlight=highlight;
            if(css.fontSize)style.font_size=parseFloat(css.fontSize)*(css.fontSize.endsWith("px")?.75:1);
            const family=(css.fontFamily||item.getAttribute("face")||"").split(",")[0].trim().replace(/^["']|["']$/g,"");
            if(notesFonts.includes(family))style.font_family=family;
            if(item.tagName==="BR"){runs.push({...style,text:"\n"});return;}
            for(const child of item.childNodes)walk(child,style);
        }
        for(const child of container.childNodes)walk(child);
        return runs;
    }
    function notesBlocksFromDOM(container){
        const blocks=[];
        function collect(child){
            if(child.nodeType===Node.TEXT_NODE){if(child.textContent.trim())blocks.push({type:notesDetectType(child.textContent),runs:[{text:child.textContent}]});return;}
            if(child.nodeType!==Node.ELEMENT_NODE)return;
            if(["SCRIPT","STYLE","IMG","IFRAME"].includes(child.tagName))return;
            if(["UL","OL"].includes(child.tagName)){for(const item of child.children)collect(item);return;}
            if(child.querySelector("p,div,h1,h2,h3,h4,h5,h6,li")){
                for(const item of child.childNodes)collect(item);return;
            }
            const text=child.textContent||"";
            blocks.push({type:child.dataset.notesType||notesDetectType(text),runs:notesRuns(child),
                ...(child.style.paddingLeft?{indent:parseFloat(child.style.paddingLeft)}:{}),
                ...(child.style.marginBottom?{space_after:parseFloat(child.style.marginBottom)}:{}),
                ...(child.style.textAlign?{align:child.style.textAlign}:{}),
                ...(child.dataset.pageBreak==="true"?{page_break:true}:{})});
        }
        for(const child of container.childNodes)collect(child);
        return blocks;
    }
    function notesBlockDOM(block){
        const p=node("p");p.dataset.notesType=block.type||"paragraph";
        if(["scripture","illustration","heading","theme","text"].includes(block.type))p.dataset.notesManual="true";
        if(block.indent!==undefined)p.style.paddingLeft=block.indent+"pt";
        if(block.space_after!==undefined)p.style.marginBottom=block.space_after+"pt";
        if(block.align)p.style.textAlign=block.align;
        if(block.page_break)p.dataset.pageBreak="true";
        for(const run of block.runs||[]){
            const span=node("span",run.text);
            if(typeof run.bold==="boolean")span.style.fontWeight=run.bold?"bold":"normal";
            if(typeof run.italic==="boolean")span.style.fontStyle=run.italic?"italic":"normal";
            if(typeof run.underline==="boolean")span.style.textDecoration=run.underline?"underline":"none";
            if(run.highlight)span.style.backgroundColor=run.highlight;if(run.color)span.style.color=run.color;
            if(run.font_size)span.style.fontSize=run.font_size+"pt";
            if(run.font_family)span.style.fontFamily='"'+run.font_family+'", '+(["Times New Roman","Georgia","Cambria"].includes(run.font_family)?"serif":run.font_family==="Courier New"?"monospace":"Arial, sans-serif");
            if(run.superscript)span.style.verticalAlign="super";
            p.append(span);
        }
        if(!p.textContent)p.append(document.createElement("br"));
        return p;
    }
    function notesSourceBlocks(blocks){return (blocks||[]).filter(b=>b.type!=="image").map(block=>({
        type:notesDetectType(block.text||""),runs:(block.runs||[{text:block.text||""}]).map(run=>({text:run.text||"",
            ...((run.marks||[]).includes("strong")?{bold:true}:{}),...((run.marks||[]).includes("em")?{italic:true}:{}),
            ...((run.marks||[]).includes("u")?{underline:true}:{})}))}));}
    function notesCurrentControl(){
        const label=node("label",undefined,"bs-current-notes"),input=node("input");input.type="checkbox";
        input.checked=localSettings.currentNotes===true;input.dataset.notesCurrent="true";
        input.addEventListener("change",()=>notesSetCurrent(input.checked));label.append(input,document.createTextNode(" Current notes"));return label;
    }
    function notesSetCurrent(checked){
        localSettings.currentNotes=checked;saveLocalSettings();
        for(const input of root.querySelectorAll('[data-notes-current],#bsNotesCurrent,#bsNotesTargetCurrent'))input.checked=checked;
    }
    for(const id of ["NotesCurrent","NotesTargetCurrent"]){element(id).checked=localSettings.currentNotes===true;element(id).addEventListener("change",()=>notesSetCurrent(element(id).checked));}
    function notesWholeButton(parent,supplier){
        parent.querySelector(":scope > .bs-notes-source-actions")?.remove();
        const actions=node("div",undefined,"bs-notes-source-actions"),button=node("button","Add whole entry to Sermon Notes");button.type="button";
        button.addEventListener("click",()=>addToSermonNotes(supplier()));actions.append(button,notesCurrentControl());parent.append(actions);
    }
    function notesCompactText(text){const refs=notesReferences(text);return refs.length?refs[0].label:text;}
    function notesMetadataBlocks(note){
        const blocks=note.content.blocks,body=blocks.filter(block=>!["theme","text"].includes(block.type));
        const metadata=[["theme","Theme",note.theme,16],["text","Text",notesCompactText(note.text),12]].map(([type,label,value,size])=>{
            const previous=blocks.find(block=>block.type===type);
            return previous||{type,runs:[{text:label+": "+value,bold:true,font_size:size,font_family:"Calibri"}]};
        });return [...metadata,...body];
    }
    function notesDocument(){
        const blocks=notesBlocksFromDOM(notesEditor),value=(type,fallback)=>{
            const block=blocks.find(block=>block.type===type);
            return block?block.runs.map(run=>run.text).join("").replace(new RegExp("^\\s*"+type+"\\s*:\\s*","i"),"").trim():fallback;
        };
        return {...notesActive.note,theme:value("theme",notesActive.note.theme),text:value("text",notesActive.note.text),content:{blocks}};
    }
    function notesUpdateHeader(note=notesActive.note){
        element("NotesTheme").textContent="Theme: "+note.theme;element("NotesText").textContent="Text: "+notesCompactText(note.text);
    }
    function notesStoreDraft(){
        if(!notesActive||!notesChanged)return;
        try{localStorage.setItem(notesDraftKey+notesActive.id,JSON.stringify({version:notesActive.version,note:notesDocument()}));}
        catch(_){/* The live editor remains available if device storage is full. */}
    }
    function notesMarkChanged(){
        if(!notesActive)return;notesChanged=true;++notesSerial;notesStoreDraft();notesStatus("Saving locally…");
        clearTimeout(notesTimer);notesTimer=setTimeout(()=>saveSermonNotes().catch(error=>notesStatus(error.message,true)),650);
        notesRenderReferences();
    }
    function saveSermonNotes(){
        clearTimeout(notesTimer);
        notesSaveChain=notesSaveChain.catch(()=>{}).then(async()=>{
            if(!notesActive||!notesChanged)return;
            const id=notesActive.id,serial=notesSerial,document=notesDocument();
            try{
                let data;
                try{data=await api(apiBase+"/notes/"+id,jsonWrite("PATCH",{version:notesActive.version,note:document}));}
                catch(error){
                    if(error.status!==409)throw error;
                    data=await api(apiBase+"/notes",jsonWrite("POST",{note:{...document,theme:(document.theme+" (local copy)").slice(0,500)}}));
                    try{localStorage.removeItem(notesDraftKey+id);}catch(_){}
                    toast("Another tab changed this sermon. Your edits were saved as a separate local copy.");
                }
                if(notesActive?.id===id){
                    notesActive={...data.item,note:notesSerial===serial?data.item.note:notesDocument()};
                    notesChanged=notesSerial!==serial;
                    notesUpdateHeader();
                    if(!notesChanged){try{localStorage.removeItem(notesDraftKey+id);}catch(_){};}
                    else notesStoreDraft();
                    notesStatus(notesChanged?"Newer edits are waiting to save…":"Saved locally · "+(notesActive.conflict?"Resolve the Drive conflict.":"Use Sync Notes to back up."));
                }
            }catch(error){notesStoreDraft();throw error;}
        });return notesSaveChain;
    }
    async function loadSermonNotes(query=element("NotesSearch").value){
        const data=await api(apiBase+"/notes?"+new URLSearchParams({q:query}));notesItems=data.notes||[];notesLoaded=true;
        element("NotesSync").disabled=!data.drive_configured;
        element("NotesSync").title=data.drive_configured?"Back up and restore your notes":"Drive connection is not configured";
        renderSermonNotesList();
        if(element("NotesStatus").dataset.error==="true")notesStatus("");
        if(!notesActive){element("NotesLibrary").hidden=false;notesStatus("Create a sermon or select a saved note. Sync Notes restores your Drive library.");}
    }
    function renderSermonNotesList(){
        const list=element("NotesList");list.replaceChildren();
        root.querySelectorAll("[data-notes-view]").forEach(button=>button.setAttribute("aria-pressed",String(button.dataset.notesView===notesLibraryView)));
        const selectedBook=element("NotesSelectedBook");selectedBook.hidden=!notesLibraryBook;
        selectedBook.textContent=notesLibraryBook?books[notesLibraryBook-1].name+" ×":"";
        selectedBook.setAttribute("aria-label",notesLibraryBook?"Clear book filter: "+books[notesLibraryBook-1].name:"Clear book filter");
        const filtered=notesItems.filter(item=>{
            const id=item.primary_reference?.book_id;
            return (notesLibraryView==="recent"||(notesLibraryView==="review"?item.needs_review:
                !item.needs_review&&(notesLibraryView==="old"?id>=1&&id<=39:id>=40&&id<=66)))&&
                (!notesLibraryBook||id===notesLibraryBook);
        });
        if(notesLibraryView==="recent")filtered.sort((a,b)=>(b.updated_at||"").localeCompare(a.updated_at||"")||a.theme.localeCompare(b.theme)||a.id.localeCompare(b.id));
        let group="";
        for(const item of filtered){
            const title=item.needs_review?"For Review":books[(item.primary_reference?.book_id||1)-1]?.name;
            if(notesLibraryView!=="recent"&&notesLibraryView!=="review"&&title!==group){list.append(node("h3",title));group=title;}
            const button=node("button",undefined,"bs-notes-list-item");button.type="button";
            button.append(node("strong",item.theme),node("span",item.text),node("small",item.conflict?"Resolve Drive conflict":item.dirty?"Saved locally · Not yet backed up":"Synced"));
            button.addEventListener("click",()=>openSermonNote(item.id).catch(error=>notesStatus(error.message,true)));list.append(button);
        }
        if(!filtered.length)list.append(node("p","No matching notes. Create Notes or use Sync Notes to restore your library."));
    }
    function notesChooseBook(){
        const old=notesLibraryView==="old",grid=element("NotesBooksGrid");grid.replaceChildren();
        element("NotesBooksTitle").textContent=old?"Old Testament":"New Testament";
        element("NotesAllBooks").textContent=old?"All OT books":"All NT books";
        element("NotesAllBooks").setAttribute("aria-current",String(!notesLibraryBook));
        books.slice(old?0:39,old?39:66).forEach((book,offset)=>{
            const index=(old?0:39)+offset,button=node("button",abbreviations[index]);button.type="button";
            button.title=book.name;button.setAttribute("aria-label",book.name);
            button.dataset.bookGroup=index<5?"law":index<17?"history":index<22?"poetry":index<39?"prophets":index<44?"gospel":index<65?"letters":"apocalypse";
            if(index+1===notesLibraryBook)button.setAttribute("aria-current","true");
            button.addEventListener("click",()=>{notesLibraryBook=index+1;renderSermonNotesList();element("NotesBooksDialog").close();});grid.append(button);
        });
        element("NotesBooksDialog").showModal();grid.parentElement.scrollTop=0;
    }
    async function openSermonNote(id){
        const opening=++notesOpenRequest;
        await saveSermonNotes();
        const serial=notesSerial;
        const data=await api(apiBase+"/notes/"+id);
        if(opening!==notesOpenRequest)return;
        if(notesSerial!==serial&&notesActive?.id===id)return;
        if(notesChanged)await saveSermonNotes();
        if(opening!==notesOpenRequest)return;
        notesActive=data.item;notesChanged=false;notesRange=null;
        let draft;try{draft=JSON.parse(localStorage.getItem(notesDraftKey+id)||"null");}catch(_){}
        if(draft?.note){
            if(draft.version===notesActive.version){notesActive.note=draft.note;notesChanged=true;++notesSerial;}
            else{
                const recovered=await api(apiBase+"/notes",jsonWrite("POST",{note:{...draft.note,theme:(draft.note.theme+" (recovered draft)").slice(0,500)}}));
                localStorage.removeItem(notesDraftKey+id);notesActive=recovered.item;toast("Recovered an unsaved device draft as a separate sermon.");
            }
        }
        renderSermonNote();selectReaderTab("notes");
        notesStatus(notesChanged?"Recovered an unsaved device draft. Press Save to keep it.":notesActive.conflict?"This sermon has a Drive conflict. Both versions are preserved.":"Saved locally"+(notesActive.dirty?" · Use Sync Notes to back up.":" · Synced"));
    }
    function renderSermonNote(){
        element("NotesLibrary").hidden=true;element("NotesWorkspace").hidden=false;element("NotesExport").disabled=false;
        notesUpdateHeader();
        notesEditor.replaceChildren(...notesMetadataBlocks(notesActive.note).map(notesBlockDOM));
        if(!notesActive.note.content.blocks.some(block=>!["theme","text"].includes(block.type)))notesEditor.append(notesBlockDOM({type:"paragraph",runs:[]}));
        element("NotesConflict").hidden=!notesActive.conflict;
        element("NotesOrigin").textContent=notesActive.note.origin?"Imported from "+notesActive.note.origin.filename+". "+(notesActive.note.origin.format_note||""):"";
        notesReadMode=false;notesSetReadMode(false);notesRenderReferences();
    }
    function notesReferences(text){
        if(!notesReferencePattern){
            const aliases=[...books.map(b=>b.name),...Object.keys(config.referenceBooks||{}),...abbreviations];
            const patterns=[...new Set(aliases)].sort((a,b)=>b.length-a.length).map(name=>name.replace(/[.*+?^${}()|[\]\\]/g,"\\$&").replace(/\s+/g,"\\s*").replace(/^(iii|ii|i|[1-3])(?=[a-z])/i,"$1\\s*"));
            notesReferencePattern=new RegExp("(?<![\\w])("+patterns.join("|")+")\\.?\\s*(\\d{1,3}):(\\d{1,3})(?:\\s*[-–—]\\s*(?:(\\d{1,3}):)?(\\d{1,3}))?","gi");
        }
        const refs=[];
        for(const match of text.matchAll(notesReferencePattern)){
            const book=referenceBookNames.get(normalizeBookName(match[1])),chapter=Number(match[2]),verse=Number(match[3]);
            const lastChapter=Number(match[4]||chapter),lastVerse=Number(match[5]||verse),definition=books.find(b=>b.name===book);
            if(!definition||chapter<1||chapter>definition.chapters||lastChapter<chapter||lastChapter>definition.chapters||verse<1||verse>176||lastVerse<1||lastVerse>176||(lastChapter===chapter&&lastVerse<verse))continue;
            refs.push({book,chapter,verse,end_chapter:lastChapter,end_verse:lastVerse,label:match[0],
                start:Array.from(text.slice(0,match.index)).length,end:Array.from(text.slice(0,match.index+match[0].length)).length});
        }return refs;
    }
    function notesRenderReferences(){
        if(!notesActive)return;const list=element("NotesReferences");list.replaceChildren();
        notesInlineReferences=[];
        // Keep links out of the editable DOM: live Ranges preserve the caret,
        // run formatting and the browser's undo history, including on touch.
        for(const block of notesEditor.childNodes){
            const text=block.textContent||"";
            for(const reference of notesReferences(text)){
                const range=notesReferenceRange(block,text,reference);
                if(range)notesInlineReferences.push({reference,range});
            }
        }
        if(window.CSS?.highlights&&typeof window.Highlight==="function"){
            CSS.highlights.set("sermon-references",new Highlight(...notesInlineReferences.map(item=>item.range)));
        }
        const text=notesActive.note.text+"\n"+notesEditor.innerText,seen=new Set();
        for(const reference of notesReferences(text)){
            if(seen.has(reference.label))continue;seen.add(reference.label);
            const button=node("button",reference.label,"bs-scripture-ref");button.type="button";
            button.addEventListener("click",()=>{
                const item=!notesReadMode&&notesInlineReferences.find(item=>item.reference.label===reference.label);
                if(item)notesPreviewEditorReference(item);else previewVerse(reference);
            });list.append(button);
        }
        if(list.children.length)list.prepend(node("small","Recognized verses: "));
    }
    function notesReferenceRange(container,text,reference){
        const start=Array.from(text).slice(0,reference.start).join("").length,end=Array.from(text).slice(0,reference.end).join("").length;
        const walker=document.createTreeWalker(container,NodeFilter.SHOW_TEXT),range=document.createRange();let offset=0,first=false;
        const nodes=container.nodeType===Node.TEXT_NODE?[container]:[];let current;
        while((current=walker.nextNode()))nodes.push(current);
        for(current of nodes){
            const next=offset+current.textContent.length;
            if(!first&&start<=next){range.setStart(current,start-offset);first=true;}
            if(first&&end<=next){range.setEnd(current,end-offset);return range;}offset=next;
        }return null;
    }
    function notesReferenceAt(x,y){
        return notesInlineReferences.find(item=>[...item.range.getClientRects()].some(rect=>
            rect.width>0&&rect.height>0&&x>=rect.left&&x<=rect.right&&y>=rect.top&&y<=rect.bottom));
    }
    function notesPreviewEditorReference(item){
        const range=item.range.cloneRange();range.collapse(false);
        previewVerse(item.reference,{noteId:notesActive.id,range,referenceRange:item.range.cloneRange(),referenceText:item.range.toString()});
    }
    notesEditor.addEventListener("pointermove",event=>{
        const item=notesReferenceAt(event.clientX,event.clientY);
        notesEditor.classList.toggle("bs-notes-reference-hover",Boolean(item));
        if(item)notesEditor.title="Preview "+item.reference.label+" · Alt+Enter at the reference";
        else notesEditor.removeAttribute("title");
    });
    notesEditor.addEventListener("pointerleave",()=>{notesEditor.classList.remove("bs-notes-reference-hover");notesEditor.removeAttribute("title");});
    notesEditor.addEventListener("pointerdown",event=>{
        clearTimeout(notesReferenceTimer);notesReferencePointer={x:event.clientX,y:event.clientY};
    });
    notesEditor.addEventListener("pointercancel",()=>{clearTimeout(notesReferenceTimer);notesReferencePointer=null;});
    notesEditor.addEventListener("compositionstart",()=>{notesComposing=true;clearTimeout(notesReferenceTimer);});
    notesEditor.addEventListener("compositionend",()=>{notesComposing=false;});
    notesEditor.addEventListener("click",event=>{
        clearTimeout(notesReferenceTimer);
        const pointer=notesReferencePointer;notesReferencePointer=null;
        if(notesComposing||event.detail>1||event.button!==0||event.ctrlKey||event.metaKey||event.altKey||event.shiftKey||window.getSelection()?.toString()||
            (pointer&&Math.hypot(event.clientX-pointer.x,event.clientY-pointer.y)>6))return;
        const item=notesReferenceAt(event.clientX,event.clientY);
        if(!item)return;
        // Let a double click or a drag select the text instead of opening a dialog.
        notesReferenceTimer=setTimeout(()=>{
            if(!notesEditor.hidden&&!element("NotesPanel").hidden&&!element("NotesWorkspace").hidden&&document.activeElement===notesEditor&&!window.getSelection()?.toString())notesPreviewEditorReference(item);
        },300);
    });
    document.addEventListener("pointerdown",event=>{if(!notesEditor.contains(event.target))clearTimeout(notesReferenceTimer);},true);
    notesEditor.addEventListener("keydown",event=>{
        clearTimeout(notesReferenceTimer);
        if(event.key!=="Enter"||!event.altKey||event.isComposing)return;
        const selection=window.getSelection();if(!selection?.isCollapsed||!selection.anchorNode)return;
        const item=notesInlineReferences.find(item=>item.range.isPointInRange(selection.anchorNode,selection.anchorOffset));
        if(item){event.preventDefault();notesPreviewEditorReference(item);}
    });
    element("Bible").addEventListener("scroll",()=>clearTimeout(notesReferenceTimer),{passive:true});
    function notesSetReadMode(read){
        notesCloseMenu();notesReadMode=read;element("NotesRead").textContent=read?"Edit Notes":"Read Notes";element("NotesRead").setAttribute("aria-pressed",String(read));
        element("NotesFormat").hidden=read;notesEditor.hidden=read;element("NotesPreview").hidden=!read;
        if(read){
            const preview=element("NotesPreview");preview.replaceChildren();
            for(const block of notesBlocksFromDOM(notesEditor)){
                const text=block.runs.map(r=>r.text).join("");
                const rendered=notesBlockDOM(block);
                for(const reference of notesReferences(text).reverse()){
                    const range=notesReferenceRange(rendered,text,reference);if(!range)continue;
                    const link=node("button",undefined,"bs-scripture-ref");link.type="button";link.append(range.extractContents());range.insertNode(link);
                    link.addEventListener("click",()=>previewVerse(reference));
                }
                preview.append(rendered);
            }
        }
    }
    document.addEventListener("selectionchange",()=>{
        const selection=window.getSelection();if(selection?.rangeCount&&notesEditor.contains(selection.anchorNode)&&notesEditor.contains(selection.focusNode)){
            if(notesMenuOpen&&selection.isCollapsed&&notesTools.contains(document.activeElement))return;
            notesRange=selection.getRangeAt(0).cloneRange();
            const parent=selection.anchorNode.nodeType===Node.ELEMENT_NODE?selection.anchorNode:selection.anchorNode.parentElement;
            if(parent){const css=getComputedStyle(parent),family=css.fontFamily.split(",")[0].replace(/^["']|["']$/g,"");
                if(notesFonts.includes(family))element("NotesFont").value=family;
                notesDisplaySize(Math.round(parseFloat(css.fontSize)*.75*10)/10);}
            if(!selection.isCollapsed)notesShowSelectionTools();
            else if(performance.now()>=notesToolbarPointerUntil&&!notesTools.contains(document.activeElement))notesCloseMenu();
        }else if(performance.now()>=notesToolbarPointerUntil&&!notesTools.contains(document.activeElement))notesCloseMenu();
    });
    function notesNormalizeFontSizes(){
        const size=Math.max(6,Math.min(72,Number(element("NotesFontSize").value)||12));
        for(const font of notesEditor.querySelectorAll("font[size]")){font.style.fontSize=size+"pt";font.removeAttribute("size");}
    }
    notesEditor.addEventListener("input",()=>{
        notesNormalizeFontSizes();
        const metadata=new Set();
        for(const child of notesEditor.children)if(["theme","text"].includes(child.dataset.notesType)){
            const type=child.dataset.notesType;
            if(metadata.has(type)){child.dataset.notesType="paragraph";delete child.dataset.notesManual;}
            else metadata.add(type);
        }
        for(const child of notesEditor.children)if(!child.dataset.notesManual)child.dataset.notesType=notesDetectType(child.textContent||"");
        notesUpdateHeader(notesDocument());
        notesMarkChanged();
    });
    notesEditor.addEventListener("paste",event=>{event.preventDefault();document.execCommand("insertText",false,event.clipboardData?.getData("text/plain")||"");notesMarkChanged();});
    element("NotesFormat").addEventListener("mousedown",event=>{if(event.target.closest("button"))event.preventDefault();});
    function notesRestoreSelection(){
        notesEditor.focus({preventScroll:true});if(notesRange&&notesEditor.contains(notesRange.commonAncestorContainer)){const selection=window.getSelection();selection.removeAllRanges();selection.addRange(notesRange);}
    }
    function notesFormat(command,value){
        if(!notesActive||notesReadMode)return;const focus=document.activeElement;notesRestoreSelection();document.execCommand(command,false,value);notesMarkChanged();notesParkFormattingFocus(focus);
    }
    element("NotesFormat").addEventListener("click",event=>{
        const command=event.target.closest("[data-notes-command]")?.dataset.notesCommand;if(!command)return;
        notesFormat(command,command==="hiliteColor"?element("NotesHighlightColor").value:command==="foreColor"?element("NotesTextColor").value:undefined);
    });
    element("NotesFont").addEventListener("change",()=>notesFormat("fontName",element("NotesFont").value));
    function notesApplySize(value){
        if(!notesActive)return;const size=Math.max(6,Math.min(72,Number(value)||12));
        const focus=document.activeElement;notesDisplaySize(size);notesRestoreSelection();document.execCommand("styleWithCSS",false,false);document.execCommand("fontSize",false,"7");
        notesNormalizeFontSizes();notesMarkChanged();notesParkFormattingFocus(focus);
    }
    element("NotesFontSize").addEventListener("change",()=>notesApplySize(element("NotesFontSize").value));
    const notesSizeSteps=[6,8,9,10,11,12,14,16,18,20,22,24,26,28,36,48,72];
    element("NotesGrow").addEventListener("click",()=>notesApplySize(notesSizeSteps.find(size=>size>Number(element("NotesFontSize").value))||72));
    element("NotesShrink").addEventListener("click",()=>notesApplySize([...notesSizeSteps].reverse().find(size=>size<Number(element("NotesFontSize").value))||6));
    element("NotesHighlightColor").addEventListener("change",()=>notesFormat("hiliteColor",element("NotesHighlightColor").value));
    element("NotesTextColor").addEventListener("change",()=>notesFormat("foreColor",element("NotesTextColor").value));
    element("NotesCase").addEventListener("change",()=>{
        const mode=element("NotesCase").value;element("NotesCase").value="";
        if(!notesActive||!mode||!notesRange||notesRange.collapsed)return;
        notesRestoreSelection();const range=window.getSelection().getRangeAt(0),walker=document.createTreeWalker(notesEditor,NodeFilter.SHOW_TEXT),pieces=[];let current;
        while((current=walker.nextNode()))if(range.intersectsNode(current)){
            const start=current===range.startContainer?range.startOffset:0,end=current===range.endContainer?range.endOffset:current.textContent.length;
            pieces.push({node:current,start,end});
        }
        let sentenceStart=true,wordStart=true;
        for(const piece of pieces){
            const source=piece.node.textContent.slice(piece.start,piece.end);
            let value=mode==="upper"?source.toLocaleUpperCase():source.toLocaleLowerCase();
            if(mode==="title")value=Array.from(value).map(char=>{const result=wordStart?char.toLocaleUpperCase():char;wordStart=!/\p{L}/u.test(char);return result;}).join("");
            if(mode==="sentence")value=Array.from(value).map(char=>{let result=char;if(sentenceStart&&/\p{L}/u.test(char)){result=char.toLocaleUpperCase();sentenceStart=false;}if(/[.!?]/.test(char))sentenceStart=true;return result;}).join("");
            piece.node.textContent=piece.node.textContent.slice(0,piece.start)+value+piece.node.textContent.slice(piece.end);piece.newEnd=piece.start+value.length;
        }
        if(pieces.length){const selected=document.createRange();selected.setStart(pieces[0].node,pieces[0].start);selected.setEnd(pieces.at(-1).node,pieces.at(-1).newEnd);notesRange=selected;const selection=window.getSelection();selection.removeAllRanges();selection.addRange(selected);}
        notesUpdateHeader(notesDocument());notesMarkChanged();notesParkFormattingFocus(element("NotesCase"));
    });
    function notesOpenMeta(edit=false,blocks=[],field="text"){
        notesEditingMeta=edit;notesPendingBlocks=blocks;
        element("NotesMetaTitle").textContent=element("NotesMetaSubmit").textContent=edit?"Save Text & Theme":"Create Notes";
        element("NotesMetaText").value=edit?notesActive.note.text:"";element("NotesMetaTheme").value=edit?notesActive.note.theme:"";element("NotesMetaError").textContent="";
        notesCloseMenu();element("NotesMetaDialog").showModal();element(field==="theme"?"NotesMetaTheme":"NotesMetaText").focus();
    }
    element("NotesCreate").addEventListener("click",()=>notesOpenMeta());
    root.querySelectorAll("[data-notes-edit-meta]").forEach(button=>button.addEventListener("click",()=>notesOpenMeta(true,[],button.dataset.notesEditMeta)));
    element("NotesMetaForm").addEventListener("submit",async event=>{
        event.preventDefault();const button=element("NotesMetaSubmit");button.disabled=true;
        try{
            await saveSermonNotes();
            const theme=element("NotesMetaTheme").value.trim(),text=element("NotesMetaText").value.trim();
            if(!theme||!text)throw Error("Enter Text and Theme first.");
            if(notesEditingMeta){
                notesActive.note={...notesActive.note,theme,text};
                for(const [type,label,value] of [["theme","Theme",theme],["text","Text",text]]){
                    const block=notesEditor.querySelector('[data-notes-type="'+type+'"]');
                    if(block){const style=notesRuns(block)[0]||{};block.replaceChildren(...notesBlockDOM({type,runs:[{...style,text:label+": "+value}]}).childNodes);}
                }
                notesMarkChanged();await saveSermonNotes();
            }
            else{
                const data=await api(apiBase+"/notes",jsonWrite("POST",{note:{schema_version:1,theme,text,content:{blocks:notesPendingBlocks}}}));
                notesActive=data.item;notesChanged=false;notesRange=null;
            }
            notesPendingBlocks=[];element("NotesMetaDialog").close();renderSermonNote();selectReaderTab("notes");
            notesStatus("Saved locally · Use Sync Notes to back up.");
        }catch(error){element("NotesMetaError").textContent=error.message;}
        finally{button.disabled=false;}
    });
    async function notesInsertBlocks(blocks,insertionRange=null){
        if(!notesActive)throw Error("Open a sermon first.");
        const targetRange=insertionRange||notesRange;
        selectReaderTab("notes");notesSetReadMode(false);notesEditor.focus();
        const selection=window.getSelection();selection.removeAllRanges();
        if(targetRange&&notesEditor.contains(targetRange.commonAncestorContainer)){const range=targetRange.cloneRange();range.collapse(false);selection.addRange(range);}
        else{const range=document.createRange();range.selectNodeContents(notesEditor);range.collapse(false);selection.addRange(range);}
        const container=node("div");for(const block of blocks)container.append(notesBlockDOM(block));
        // Paragraph boundaries prevent the browser from merging a verse into
        // a bold Text header or swallowing the words after the reference.
        const html=insertionRange?"<p><br></p>"+container.innerHTML+"<p><br></p>":container.innerHTML;
        document.execCommand("insertHTML",false,html);notesMarkChanged();await saveSermonNotes();
        toast("Added to sermon notes. Use Undo to remove the insertion.");
    }
    async function addToSermonNotes(blocks){
        try{
            if(localSettings.currentNotes===true&&notesActive){await notesInsertBlocks(blocks);return;}
            notesPendingBlocks=blocks;await loadSermonNotes("");
            const select=element("NotesTargetExisting");select.replaceChildren();
            for(const item of notesItems.filter(n=>!n.deleted)){const option=node("option",item.theme+" · "+item.text);option.value=item.id;select.append(option);}
            if(notesActive&&[...select.options].some(o=>o.value===notesActive.id))select.value=notesActive.id;
            element("NotesTargetAdd").disabled=!select.options.length;element("NotesTargetError").textContent="";element("NotesTargetDialog").showModal();
        }catch(error){toast(error.message);}
    }
    element("NotesTargetNew").addEventListener("click",()=>{element("NotesTargetDialog").close();notesOpenMeta(false,notesPendingBlocks);});
    element("NotesTargetAdd").addEventListener("click",async()=>{
        const button=element("NotesTargetAdd");button.disabled=true;
        try{const blocks=notesPendingBlocks;await openSermonNote(element("NotesTargetExisting").value);element("NotesTargetDialog").close();await notesInsertBlocks(blocks);notesPendingBlocks=[];}
        catch(error){element("NotesTargetError").textContent=error.message;}finally{button.disabled=false;}
    });
    element("NotesInsertVerse").addEventListener("click",async()=>{
        const blocks=notesPlainBlocks(element("VerseText").textContent,"scripture"),target=notesVerseTarget;
        notesVerseTarget=null;element("VerseDialog").close();
        if(!target){addToSermonNotes(blocks);return;}
        try{
            if(target.noteId!==notesActive?.id||element("NotesWorkspace").hidden||element("NotesPanel").hidden||
                !notesEditor.contains(target.range.commonAncestorContainer)||target.referenceRange.toString()!==target.referenceText)
                throw Error("This sermon changed. Open the reference again before inserting its verses.");
            await notesInsertBlocks(blocks,target.range);
        }catch(error){notesStatus(error.message,true);}
    });
    element("VerseDialog").addEventListener("close",()=>{if(!element("VerseDialog").open)notesVerseTarget=null;});
    element("NotesAll").addEventListener("click",async()=>{
        try{await saveSermonNotes();await loadSermonNotes();element("NotesLibrary").hidden=false;element("NotesWorkspace").hidden=true;}
        catch(error){notesStatus(error.message,true);}
    });
    root.querySelectorAll("[data-notes-view]").forEach(button=>button.addEventListener("click",()=>{
        const view=button.dataset.notesView;
        if(view!==notesLibraryView)notesLibraryBook=0;
        notesLibraryView=view;renderSermonNotesList();
        if(view==="old"||view==="new")notesChooseBook();
    }));
    element("NotesSelectedBook").addEventListener("click",()=>{notesLibraryBook=0;renderSermonNotesList();});
    element("NotesAllBooks").addEventListener("click",()=>{notesLibraryBook=0;renderSermonNotesList();element("NotesBooksDialog").close();});
    let notesSearchTimer=null;
    element("NotesSearch").addEventListener("input",()=>{clearTimeout(notesSearchTimer);notesSearchTimer=setTimeout(()=>loadSermonNotes().catch(error=>notesStatus(error.message,true)),250);});
    element("NotesRead").addEventListener("click",()=>notesSetReadMode(!notesReadMode));
    element("NotesSave").addEventListener("click",()=>saveSermonNotes().catch(error=>notesStatus(error.message,true)));
    function notesConfirm(message,action,{title="Confirm",confirmLabel="Continue",danger=false}={}){
        const dialog=element("NotesConfirmDialog"),button=element("NotesConfirmYes");
        element("NotesConfirmText").textContent=message;element("NotesConfirmTitle").textContent=title;
        button.textContent=confirmLabel;button.classList.toggle("bs-notes-delete-confirm",danger);
        button.onclick=async()=>{if(!dialog.open||button.disabled)return;button.disabled=true;try{await action();dialog.close();}catch(error){notesStatus(error.message,true);dialog.close();}finally{button.disabled=false;}};
        notesCloseMenu();dialog.showModal();element("NotesConfirmCancel").focus();
    }
    element("NotesDelete").addEventListener("click",()=>{
        if(!notesActive)return;const id=notesActive.id,theme=notesChanged?notesDocument().theme:notesActive.note.theme;
        notesConfirm('Are you sure you want to delete “'+theme+'”? Sync Notes will also remove its current copy from your other devices.',async()=>{
        await saveSermonNotes();if(notesActive?.id!==id)throw Error("The open sermon changed. Reopen the sermon you want to delete.");
        await api(apiBase+"/notes/"+id,jsonWrite("DELETE",{version:notesActive.version}));
        notesActive=null;notesRange=null;notesChanged=false;element("NotesWorkspace").hidden=true;element("NotesExport").disabled=true;await loadSermonNotes();
        },{title:"Delete sermon notes?",confirmLabel:"Delete sermon",danger:true});
    });
    async function notesResolve(choice){
        await saveSermonNotes();const id=notesActive.id;const result=await api(apiBase+"/notes/"+id+"/resolve",jsonWrite("POST",{version:notesActive.version,choice}));
        notesActive=null;notesChanged=false;await loadSermonNotes();
        const target=result.copy_id||notesItems.find(n=>n.id===id&&!n.deleted)?.id;
        if(target)await openSermonNote(target);else{element("NotesWorkspace").hidden=true;element("NotesExport").disabled=true;}
    }
    element("NotesKeepBoth").addEventListener("click",()=>notesResolve("copy").catch(error=>notesStatus(error.message,true)));
    element("NotesUseRemote").addEventListener("click",()=>notesConfirm("Use the Drive version and discard the conflicting local copy?",()=>notesResolve("remote")));
    async function notesPollJob(){
        try{
            const data=await api(apiBase+"/notes/job"),job=data.job;notesRenderJob(job);
            if(job.running){notesJobTimer=setTimeout(notesPollJob,1500);return;}
            await loadSermonNotes();
            if(notesActive&&!notesChanged){
                try{await openSermonNote(notesActive.id);}
                catch(error){if(error.status!==404)throw error;notesActive=null;element("NotesWorkspace").hidden=true;element("NotesLibrary").hidden=false;element("NotesExport").disabled=true;}
            }
        }catch(error){notesStatus(error.message,true);element("NotesSync").disabled=false;}
    }
    function notesRenderJob(job){
        element("NotesJob").hidden=false;element("NotesJobMessage").textContent=job.message||"";
        const progress=element("NotesProgress");progress.hidden=!job.running;if(job.total)progress.value=job.completed/job.total*100;else progress.removeAttribute("value");
        element("NotesJobErrors").replaceChildren(...(job.errors||[]).map(e=>node("li",e.filename+": "+e.error)));
        element("NotesSync").disabled=job.running;
    }
    async function notesStartJob(action){
        try{await saveSermonNotes();clearTimeout(notesJobTimer);const data=await api(apiBase+"/notes/"+action,jsonWrite("POST",{}));notesRenderJob(data.job);await notesPollJob();}
        catch(error){notesStatus(error.message,true);}
    }
    element("NotesSync").addEventListener("click",()=>notesStartJob("sync"));
    element("NotesExport").addEventListener("click",async()=>{
        try{
            await saveSermonNotes();if(!notesActive)return;
            element("NotesExportDialog").showModal();element("NotesExportStatus").textContent="Preparing PDF…";element("NotesDownload").disabled=element("NotesShare").disabled=true;
            const response=await fetch(apiBase+"/notes/"+notesActive.id+"/pdf",{credentials:"same-origin",cache:"no-store"});
            if(!response.ok||!(response.headers.get("Content-Type")||"").includes("application/pdf"))throw Error("Could not export this sermon. Your local notes are saved.");
            if(notesPDFUrl)URL.revokeObjectURL(notesPDFUrl);
            const filename=(response.headers.get("Content-Disposition")||"").match(/filename="([^"]+)"/)?.[1]||"sermon-notes.pdf";
            notesPDF=new File([await response.blob()],filename,{type:"application/pdf"});notesPDFUrl=URL.createObjectURL(notesPDF);
            element("NotesDownload").disabled=element("NotesShare").disabled=false;element("NotesExportStatus").textContent="PDF ready. Download to this device or share it.";
        }catch(error){if(element("NotesExportDialog").open)element("NotesExportStatus").textContent=error.message;else notesStatus(error.message,true);}
    });
    function notesDownload(){if(!notesPDFUrl)return;const link=node("a");link.href=notesPDFUrl;link.download=notesPDF.name;document.body.append(link);link.click();link.remove();}
    element("NotesDownload").addEventListener("click",notesDownload);
    element("NotesShare").addEventListener("click",async()=>{
        if(!notesPDF)return;
        try{if(navigator.share&&(!navigator.canShare||navigator.canShare({files:[notesPDF]})))await navigator.share({files:[notesPDF],title:notesPDF.name});
            else{notesDownload();element("NotesExportStatus").textContent="File sharing is unavailable here, so the PDF was downloaded.";}}
        catch(error){element("NotesExportStatus").textContent=error.name==="AbortError"?"Sharing cancelled.":"Could not share the PDF. You can still download it.";}
    });
    root.querySelectorAll("[data-notes-close]").forEach(button=>button.addEventListener("click",()=>element(button.dataset.notesClose).close()));
    window.addEventListener("beforeunload",event=>{if(notesChanged){notesStoreDraft();event.preventDefault();event.returnValue="";}if(notesPDFUrl)URL.revokeObjectURL(notesPDFUrl);});

    function selectReaderTab(name, focus = false) {
        if (!["bible", "book", "notes"].includes(name) || name === readerTab) return;
        window.getSelection()?.removeAllRanges(); clearSelectionActions();
        readerScroll[readerTab] = element("Bible").scrollTop;
        readerTab = name; scrollRestoreUntil = performance.now() + 500;
        element("ReaderPanel").hidden = name !== "bible"; element("BookStudyPanel").hidden = name !== "book";
        element("NotesPanel").hidden = name !== "notes";
        for (const [tab, prefix] of [["bible", "Reader"], ["book", "BookStudy"], ["notes", "Notes"]]) {
            element(prefix + "Tab").setAttribute("aria-selected", String(name === tab));
            element(prefix + "Tab").tabIndex = name === tab ? 0 : -1;
        }
        element("Bible").scrollTop = readerScroll[name];
        if (name === "bible" && readerPassageChanged) {
            readerPassageChanged = false;
            const target = element("Verse" + state.verse);
            if (target && state.verse > 1) element("Bible").scrollTop += target.getBoundingClientRect().top - element("Bible").getBoundingClientRect().top - readerChromeHeight() - 20;
        }
        positionWorkspaceTools();
        if (name === "book") loadBookStudy();
        if (name === "notes" && !notesLoaded) loadSermonNotes().catch(error=>notesStatus(error.message,true));
        if (focus) element(({bible:"ReaderTab",book:"BookStudyTab",notes:"NotesTab"})[name]).focus();
    }
    root.querySelectorAll("[data-reader-tab]").forEach(button => {
        button.addEventListener("click", () => selectReaderTab(button.dataset.readerTab));
        button.addEventListener("keydown", event => {
            if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
            const tabs=["bible","book","notes"],index=tabs.indexOf(readerTab);
            event.preventDefault(); selectReaderTab(event.key === "Home" ? "bible" : event.key === "End" ? "notes" : tabs[(index+(event.key==="ArrowRight"?1:2))%3], true);
        });
    });

    async function previewVerse(reference,notesTarget=null) {
        lastVerseReference = reference;
        notesVerseTarget=notesTarget;
        const requestId = ++verseRequest;
        copiedVerse = ""; element("CopyVerse").disabled = true; element("NotesInsertVerse").disabled=true;
        element("CopyVerse").textContent = "Copy Verse";
        element("VerseReference").textContent = reference.book + " " + reference.chapter + ":" + reference.verse;
        element("VerseText").textContent = "Loading verse…"; element("VerseAttribution").textContent = "";
        if (!element("VerseDialog").open) element("VerseDialog").showModal();
        try {
            const data = await api(config.verseUrl + "?" + new URLSearchParams({translation: state.translation, book: reference.book, chapter: reference.chapter, verse: reference.verse,
                end_chapter: reference.end_chapter || reference.chapter, end_verse: reference.end_verse || reference.verse}));
            if (requestId !== verseRequest) return;
            element("VerseReference").textContent = data.verse.reference;
            element("VerseText").textContent = data.verse.text;
            element("VerseAttribution").textContent = data.verse.attribution;
            copiedVerse = data.verse.reference + " (" + (data.verse.edition_label || translationLabel()) + ")\n" + data.verse.text;
            element("CopyVerse").disabled = false;
            element("NotesInsertVerse").disabled=false;
        } catch (error) {
            if (requestId === verseRequest) element("VerseText").textContent = error.message;
        }
    }

    function renderChain(topic, scrollTop = 0) {
        currentChain = topic;
        element("ChainBack").disabled = false;
        element("ChainBack").hidden = chainHistory.length === 0;
        element("ChainTitle").textContent = topic.key + " · " + topic.title;
        const blocks = topic.blocks.filter((block, index) =>
            !(index === 0 && block.role === "intro" && block.text.replace(/[\s,.]+$/, "") === topic.title.replace(/[\s,.]+$/, "")));
        element("ChainContent").replaceChildren(...blocks.map(block => {
            const paragraph = renderSourceBlock(block);
            if (["intro", "section", "subheading", "references", "verse", "example", "related"].includes(block.role)) paragraph.classList.add("bs-chain-" + block.role);
            return paragraph;
        }));
        for (const page of topic.pages || []) {
            const image = document.createElement("img"); image.className = "bs-chain-chart";
            image.src = apiBase + "/studies/page/" + page; image.alt = topic.title + " · Source page " + page;
            image.loading = "lazy"; element("ChainContent").append(image);
        }
        element("ChainSource").textContent = (topic.source_label || "Thompson Chain-Reference Bible · Numerical index") + (topic.source_page ? " · PDF page " + topic.source_page : "");
        element("ChainDialog").scrollTop = scrollTop;
    }
    async function previewChain(topicId, follow = false, referenceKey = null) {
        const requestId = ++chainRequest;
        const previous = follow && currentChain ? {topic: currentChain, scrollTop: element("ChainDialog").scrollTop} : null;
        if (!follow) { chainHistory.length = 0; currentChain = null; }
        element("ChainBack").hidden = chainHistory.length === 0;
        element("ChainTitle").textContent = "Chain topic";
        element("ChainBack").disabled = true;
        element("ChainContent").replaceChildren(node("p", "Loading topic…")); element("ChainSource").textContent = "";
        if (!element("ChainDialog").open) element("ChainDialog").showModal();
        try {
            const data = await api(apiBase + (topicId ? "/chain/" + topicId : "/thompson-reference/" + encodeURIComponent(referenceKey)));
            if (requestId !== chainRequest || !element("ChainDialog").open) return;
            if (previous) chainHistory.push(previous);
            renderChain(data.topic);
        } catch (error) {
            if (requestId !== chainRequest) return;
            if (previous) {
                renderChain(previous.topic, previous.scrollTop);
                const message = node("p", error.message); message.setAttribute("role", "status"); element("ChainContent").prepend(message);
            } else element("ChainContent").replaceChildren(node("p", error.message));
        }
    }
    element("ChainBack").addEventListener("click", () => {
        const previous = chainHistory.pop();
        if (previous) { ++chainRequest; renderChain(previous.topic, previous.scrollTop); }
    });
    element("ChainDialog").addEventListener("close", () => { ++chainRequest; currentChain = null; chainHistory.length = 0; });
    element("VerseDialog").addEventListener("close", () => { if (!element("VerseDialog").open) ++verseRequest; });
    root.querySelectorAll("dialog").forEach(dialog => {
        dialog.addEventListener("close",()=>{
            if(element("SelectionToolbar").parentElement===dialog){root.append(element("SelectionToolbar"));clearSelectionActions();}
            if(element("Toast").parentElement===dialog)([...root.querySelectorAll("dialog[open]")].at(-1)||root).append(element("Toast"));
        });
        let outside = false;
        function isOutside(event) {
            const bounds = dialog.getBoundingClientRect();
            return event.target === dialog && (event.clientX < bounds.left || event.clientX > bounds.right || event.clientY < bounds.top || event.clientY > bounds.bottom);
        }
        dialog.addEventListener("pointerdown", event => { outside = isOutside(event); });
        dialog.addEventListener("click", event => {
            if (outside && isOutside(event)) { event.stopPropagation(); dialog.close(); }
            outside = false;
        });
    });

    element("CopyVerse").addEventListener("click", async () => {
        await copyScripture(copiedVerse);
    });

    root.querySelectorAll("[data-study-image-action]").forEach(button=>button.addEventListener("click",async()=>{
        const match=element("SourceImage").src.match(/\/studies\/page\/(\d+)/);
        if(!match)return;
        button.disabled=true;
        try {
            const data=await api(apiBase+"/study-image?page="+match[1]);const anchor=data.anchor;
            pendingSelection={study:true,reference:anchor.title,ranges:[{token:anchor.token,text:anchor.text,start_offset:0,end_offset:Array.from(anchor.text).length,selected_text:anchor.text}]};
            if(button.dataset.studyImageAction==="note")openNote();
            else {
                await api(apiBase+"/study-saves",jsonWrite("POST",{kind:"bookmark",note:"",color:null,ranges:pendingSelection.ranges}));
                clearSelectionActions();await loadPersonal();toast("Study page bookmarked.");
            }
        } catch(error){toast(error.message);}finally{button.disabled=false;}
    }));

    function showSyncReport(report) {
        syncBusy = report.status === "running";
        element("Sync").disabled = syncBusy;
        element("Sync").textContent = syncBusy ? "Syncing Bible Study…" : "Sync Bible Study";
        element("SyncDetails").textContent = syncBusy ? "Sync details · Running" : report.status === "failed" ? "Sync details · Failed" : "Sync details";
        element("SyncStatus").textContent = report.message;
        const state=report.progress || {}, panel=element("SyncProgressPanel"),bar=element("SyncProgress");
        panel.hidden=!report.progress && !syncBusy;
        const labels={starting:"Starting",discovery:"Finding sources",preparing:"Preparing sources",checking:"Checking source",downloading:"Downloading",indexing:"Indexing",prepared:"Source prepared",validating:"Validating database",backup:"Backing up",publishing:"Publishing",illustrations:"Checking illustrations",translations:"Bible translations"};
        element("SyncStage").textContent=report.status==="completed"?"Sync complete":report.status==="failed"?"Sync stopped":report.status==="interrupted"?"Sync interrupted":labels[state.stage] || state.stage || "Working";
        element("SyncSource").textContent=state.source || "";
        const done=Number(state.completed),total=Number(state.total),measured=state.completed!=null&&state.total!=null&&Number.isFinite(done)&&Number.isFinite(total)&&total>0;
        if(report.status==="completed"){bar.value=100;element("SyncMeasure").textContent="All sync stages completed.";}
        else if(measured){
            const percent=Math.min(100,Math.max(0,done/total*100));bar.value=percent;
            const amount=value=>state.unit==="bytes"?value.toLocaleString()+" bytes ("+(value/1048576).toLocaleString(undefined,{maximumFractionDigits:2})+" MB)":value.toLocaleString();
            element("SyncMeasure").textContent=amount(done)+" / "+amount(total)+(state.unit!=="bytes"?" "+(state.unit || "items"):"")+" · "+percent.toFixed(1)+"% of this stage";
        }else if(report.status!=="running"){
            bar.value=0;element("SyncMeasure").textContent="This job is no longer running.";
        }else{
            bar.removeAttribute("value");
            element("SyncMeasure").textContent=state.completed!=null&&state.unit==="bytes"?(done/1048576).toFixed(2)+" MB downloaded; total size unavailable.":"This stage has no measured total yet.";
        }
        element("SyncSources").textContent=state.sources_total!=null?(state.source_group || "Study sources prepared")+": "+Number(state.sources_done || 0).toLocaleString()+" / "+Number(state.sources_total).toLocaleString():"";
        window.dispatchEvent(new CustomEvent("bible-study-sync-status",{detail:{active:syncBusy}}));
        const body = element("SyncReportBody"); body.replaceChildren();
        for (const [key, value] of Object.entries(report.counts || {})) body.append(node("p", key.replaceAll("_", " ") + ": " + Number(value).toLocaleString()));
        for (const warning of report.warnings || []) body.append(node("p", warning));
        element("SyncReport").hidden = !body.childNodes.length;
    }

    let syncPollActive=false,syncPollAgain=false;
    async function pollSync() {
        if(syncPollActive){syncPollAgain=true;return;}
        syncPollActive=true;
        clearTimeout(syncTimer);
        try {
            const data = await api(config.syncUrl);
            showSyncReport(data.sync);
            if (data.sync.status === "running") syncTimer = setTimeout(pollSync, 2500);
            else if (data.sync.status === "completed") {
                sourceResponses.clear();sourcePending.clear();commentaryRenderedKey="";sermonChapter="";
                const status = await api(config.statusUrl);
                contentAvailable = status.indexed_verses > 0;
                storageAvailable = status.schema_ready;
                element("Status").textContent = status.message;
                element("SaveStatus").textContent = "Reading preferences can be saved to your account.";
                await refreshTranslations(); updateChapter();
            }
        } catch (error) {
            if(error.status===401 || error.status===403){
                syncBusy=false;element("Sync").disabled=false;element("SyncStatus").textContent=error.message;
            }else{
                element("SyncStatus").textContent="Progress connection interrupted. Reconnecting…";
                syncTimer=setTimeout(pollSync,5000);
            }
        } finally {
            syncPollActive=false;
            if(syncPollAgain){syncPollAgain=false;clearTimeout(syncTimer);syncTimer=setTimeout(pollSync,0);}
        }
    }

    if (element("Sync") && config.syncUrl) {
        element("SyncDetails").addEventListener("click", () => {
            if (element("SettingsDialog").open) element("SettingsDialog").close();
            element("SyncDialog").showModal();
            pollSync();
        });
        element("CloseSync").addEventListener("click", () => element("SyncDialog").close());
        element("Sync").addEventListener("click", async () => {
            if (syncBusy) return;
            if(element("SettingsDialog").open)element("SettingsDialog").close();
            if(!element("SyncDialog").open)element("SyncDialog").showModal();
            showSyncReport({status:"running",message:"Starting Bible Study sync…",progress:{stage:"starting"}});
            try {
                await api(config.syncUrl, {method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": config.csrfToken}, body: "{}"});
                pollSync();
            } catch (error) {
                // Another tab or worker owns the sync. Follow that job instead
                // of presenting its duplicate-start protection as a sync failure.
                if(error.status===409){await pollSync();return;}
                showSyncReport({status:"failed",message:error.message,progress:{stage:"starting"}});
                element("SyncDetails").textContent = "Sync details · Error";
            }
        });
    }

    function savePreferences() {
        if (!storageAvailable || writePaused) return;
        saveChain = saveChain.then(async () => {
            if (writePaused) return;
            const data = await api(config.stateUrl, {
                method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": config.csrfToken},
                body: JSON.stringify(state)
            });
            state.version = data.version;
            element("SaveStatus").textContent = "Reading preferences saved to your account.";
        }).catch(error => {
            writePaused = true;
            element("SaveStatus").textContent = error.message + " Changes in this view have not been saved. Reload to try again.";
        });
    }

    element("Bible").addEventListener("scroll", () => {
        clearTimeout(scrollTimer);
        if (readerTab !== "bible" || !storageAvailable || !contentAvailable || performance.now() < scrollRestoreUntil) return;
        scrollTimer = setTimeout(() => {
            if (readerTab !== "bible") return;
            const top = element("Bible").getBoundingClientRect().top + readerChromeHeight();
            const visible = [...element("Verses").querySelectorAll(".bs-verse")].find(verse => verse.getBoundingClientRect().bottom > top + 8);
            const number = Number(visible?.id.replace("bsVerse", ""));
            if (number && number !== state.verse) { state.verse = number; savePreferences(); }
        }, 700);
    }, {passive: true});

    function updateChapter() {
        if (readerTab === "book") readerPassageChanged = true;
        let book = books.find(item => item.name === state.book);
        if (!book) { book = books[0]; state.book = book.name; }
        state.chapter = Math.max(1, Math.min(book.chapters, Number(state.chapter) || 1));
        element("Book").value = state.book;
        element("Chapter").replaceChildren(...Array.from({length: book.chapters}, (_, index) => {
            const option = document.createElement("option"); option.value = index + 1; option.textContent = index + 1; return option;
        }));
        element("Chapter").value = state.chapter;
        element("BookHeading").textContent = state.book;
        element("ChapterHeading").textContent = state.chapter;
        element("Navigate").textContent = state.book + " " + state.chapter + " ▾";
        const index = books.indexOf(book);
        element("Previous").disabled = index === 0 && state.chapter === 1;
        element("Next").disabled = index === books.length - 1 && state.chapter === book.chapters;
        element("PreviousEnd").disabled = element("Previous").disabled;
        element("NextEnd").disabled = element("Next").disabled;
        if (bookStudyBook !== state.book) {
            ++bookStudyRequest; readerScroll.book = 0;
            if (readerTab === "book") element("Bible").scrollTop = 0;
        }
        cancelBackgroundStudy();
        ++commentaryRequest;
        ++illustrationRequest;illustrationPassage=null;illustrationSuggestionKey="";
        if (sermonChapter !== state.book+" "+state.chapter) { ++sermonRequest; sermonChapter=""; }
        const passageReady=chapterLoadPromise=loadChapter();
        if(readerTab==="book")passageReady.then(()=>{if(passageReady===chapterLoadPromise&&readerTab==="book"&&bookStudyBook!==state.book)loadBookStudy();});
        if (studyTab === "commentary") loadCommentary();
        if (studyTab === "sermon") loadSermons();
        if (studyTab === "illustrations") loadIllustrations();
        else if(studyTab!=="commentary")chapterLoadPromise.then(scheduleBackgroundStudy);
    }

    function applyAppearance() {
        root.dataset.theme = state.theme;
        root.style.setProperty("--bs-font", state.font_size + "px");
        element("Theme").value = state.theme;
        element("FontSize").value = state.font_size;
        element("FontSizeValue").textContent = state.font_size;
        root.dataset.margins = localSettings.margins === false ? "hidden" : "shown";
        element("ShowMargins").checked = localSettings.margins !== false;
        element("Smaller").disabled = state.font_size <= 14;
        element("Larger").disabled = state.font_size >= 30;
    }

    function setRatio(value) {
        const width=element("Layout").clientWidth;
        const minimum=mobile.matches || width<1?20:Math.max(20,Math.ceil(220/Math.max(1,width-12)*100));
        state.split_ratio = Math.max(minimum, Math.min(100-minimum, Math.round(value)));
        root.style.setProperty("--bs-split", state.split_ratio);
        // Keep a narrow desktop/tablet pane readable. Only its text layout
        // changes; the independent wide tab row never follows this divider.
        const readerWidth=Math.max(0,width-12)*state.split_ratio/100;
        root.dataset.compactReader=String(!mobile.matches && readerWidth<330);
        element("Divider").setAttribute("aria-valuemin",minimum);
        element("Divider").setAttribute("aria-valuemax",100-minimum);
        element("Divider").setAttribute("aria-valuenow", state.split_ratio);
        element("Divider").setAttribute("aria-orientation", mobile.matches ? "horizontal" : "vertical");
    }

    async function setMode(mode, userAction = false) {
        if (!["split", "bible", "study"].includes(mode)) mode = "split";
        state.layout_mode = mode;
        root.dataset.mode = mode;
        root.classList.toggle("bs-fullscreen", mode !== "split");
        element("LayoutMode").value = mode;
        root.querySelectorAll("[data-bs-mode]").forEach(button => button.setAttribute("aria-pressed", String(button.dataset.bsMode === mode)));
        if (mode === "split") {
            if (document.fullscreenElement === root) {
                try { await document.exitFullscreen(); } catch (_error) {}
            }
            if (userAction) previousFocus?.focus?.();
        } else if (userAction) {
            previousFocus = document.activeElement;
            element(mode === "bible" ? "Bible" : "Study").focus();
        }
        positionWorkspaceTools();sizeWorkspace();
        if (userAction) savePreferences();
    }

    function stepChapter(direction) {
        if (element(direction < 0 ? "Previous" : "Next").disabled) return;
        let index = books.findIndex(book => book.name === state.book);
        state.chapter += direction;
        if (state.chapter > books[index].chapters && index < books.length - 1) {
            state.book = books[++index].name; state.chapter = 1;
        } else if (state.chapter < 1 && index > 0) {
            state.book = books[--index].name; state.chapter = books[index].chapters;
        }
        state.verse = 1; updateChapter(); savePreferences();
    }

    // Horizontal touch gestures stay inside the Bible. Native vertical scrolling,
    // text selection and pinch zoom remain available.
    let swipe = null, suppressWordClickUntil = 0, wordPress = null, touchSwipe = null, nativeTouch = false;
    const scripture = element("Scripture");
    scripture.addEventListener("pointerdown", event => {
        if (event.isPrimary && event.target.closest(".bs-word")) wordPress = {id: event.pointerId, time: performance.now()};
        if (!event.isPrimary) { swipe = null; return; }
        if (!touchNavigation() || event.pointerType !== "touch" || readerTab !== "bible"
                || root.querySelector("dialog[open]") || window.getSelection()?.toString()) return;
        const control = event.target.closest("button,a,input,select,textarea,summary");
        if (control && !control.classList.contains("bs-word")) return;
        swipe = {id: event.pointerId, x: event.clientX, y: event.clientY, time: performance.now()};
    }, {passive: true});
    scripture.addEventListener("pointermove", event => {
        if (swipe?.id !== event.pointerId) return;
        const dx = Math.abs(event.clientX - swipe.x), dy = Math.abs(event.clientY - swipe.y);
        if (dy > 18 && dy > dx * 1.25) swipe = null;
    }, {passive: true});
    scripture.addEventListener("pointercancel", () => { swipe = null; });
    scripture.addEventListener("pointerup", event => {
        if (wordPress?.id === event.pointerId && performance.now() - wordPress.time > 400) suppressWordClickUntil = performance.now() + 650;
        wordPress = null;
        const gesture = swipe; swipe = null;
        if (nativeTouch || !gesture || gesture.id !== event.pointerId || performance.now() - gesture.time > 1100
                || window.getSelection()?.toString() || root.querySelector("dialog[open]")) return;
        const dx = event.clientX - gesture.x, dy = event.clientY - gesture.y;
        if (Math.abs(dx) < 42 || Math.abs(dx) < Math.abs(dy) * 1.15) return;
        suppressWordClickUntil = performance.now() + 400;
        stepChapter(dx < 0 ? 1 : -1);
        toast(state.book+" "+state.chapter,1200);
    }, {passive: true});
    // Touch events continue on iOS when the browser cancels pointer events.
    scripture.addEventListener("touchstart", event => {
        nativeTouch=true;touchSwipe=null;
        if(!touchNavigation()||readerTab!=="bible"||event.touches.length!==1||root.querySelector("dialog[open]")||window.getSelection()?.toString())return;
        const control=event.target.closest("button,a,input,select,textarea,summary");
        if(control&&!control.classList.contains("bs-word"))return;
        const touch=event.touches[0];touchSwipe={x:touch.clientX,y:touch.clientY,time:performance.now(),horizontal:false};
    },{passive:true});
    scripture.addEventListener("touchmove", event => {
        if(!touchSwipe||event.touches.length!==1||window.getSelection()?.toString()){touchSwipe=null;return;}
        const touch=event.touches[0],dx=Math.abs(touch.clientX-touchSwipe.x),dy=Math.abs(touch.clientY-touchSwipe.y);
        if(!touchSwipe.horizontal&&performance.now()-touchSwipe.time>450){touchSwipe=null;return;}
        if(dy>18&&dy>dx*1.25){touchSwipe=null;return;}
        if(dx>12&&dx>dy*1.15)touchSwipe.horizontal=true;
        if(touchSwipe.horizontal&&event.cancelable)event.preventDefault();
    },{passive:false});
    scripture.addEventListener("touchend", event => {
        const gesture=touchSwipe;touchSwipe=null;
        setTimeout(()=>{nativeTouch=false;},0);
        if(!gesture||event.touches.length||performance.now()-gesture.time>1100||window.getSelection()?.toString()||!event.changedTouches.length)return;
        const touch=event.changedTouches[0],dx=touch.clientX-gesture.x,dy=touch.clientY-gesture.y;
        if(Math.abs(dx)<42||Math.abs(dx)<Math.abs(dy)*1.15)return;
        suppressWordClickUntil=performance.now()+650;swipe=null;stepChapter(dx<0?1:-1);toast(state.book+" "+state.chapter,1200);
    },{passive:true});
    scripture.addEventListener("touchcancel",()=>{touchSwipe=null;nativeTouch=false;swipe=null;},{passive:true});
    scripture.addEventListener("click", event => {
        if (event.target.closest(".bs-word") && (performance.now() < suppressWordClickUntil || window.getSelection()?.toString())) {
            event.preventDefault(); event.stopImmediatePropagation();
        }
    }, true);
    root.addEventListener("click",event=>{
        if(event.target.closest(".bs-scripture-ref,.bs-chain-ref,.bs-dictionary-ref")&&(window.getSelection()?.toString()||(pendingSelection?.study&&performance.now()<suppressWordClickUntil))){event.preventDefault();event.stopImmediatePropagation();}
    },true);

    element("Book").addEventListener("change", event => {
        state.book = event.target.value; state.chapter = 1; state.verse = 1; updateChapter(); savePreferences();
    });
    element("Chapter").addEventListener("change", event => {
        state.chapter = Number(event.target.value); state.verse = 1; updateChapter(); savePreferences();
    });
    element("Previous").addEventListener("click", () => stepChapter(-1));
    element("Next").addEventListener("click", () => stepChapter(1));
    element("PreviousEnd").addEventListener("click", () => stepChapter(-1));
    element("NextEnd").addEventListener("click", () => stepChapter(1));
    const abbreviations = ["Gen", "Ex", "Lev", "Num", "Deut", "Josh", "Judg", "Ruth", "1 Sam", "2 Sam", "1 Ki", "2 Ki", "1 Chr", "2 Chr", "Ezra", "Neh", "Est", "Job", "Ps", "Prov", "Eccl", "Song", "Isa", "Jer", "Lam", "Ezek", "Dan", "Hos", "Joel", "Amos", "Obad", "Jonah", "Mic", "Nah", "Hab", "Zeph", "Hag", "Zech", "Mal", "Matt", "Mark", "Luke", "John", "Acts", "Rom", "1 Cor", "2 Cor", "Gal", "Eph", "Phil", "Col", "1 Th", "2 Th", "1 Tim", "2 Tim", "Titus", "Phlm", "Heb", "Jas", "1 Pet", "2 Pet", "1 Jn", "2 Jn", "3 Jn", "Jude", "Rev"];
    const referenceBookNames=new Map();
    const normalizeBookName=name=>name.toLowerCase().replace(/[\s.]/g,"");
    books.forEach((book,index)=>{
        for(const name of [book.name,abbreviations[index]])referenceBookNames.set(normalizeBookName(name),book.name);
    });
    for(const [alias,name] of Object.entries(config.referenceBooks || {}))referenceBookNames.set(normalizeBookName(alias),name);
    function showNavigationTab(chapter) {
        element("BookGrid").hidden = chapter; element("ChapterGrid").hidden = !chapter;
        element("BookTab").setAttribute("aria-selected", String(!chapter));
        element("ChapterTab").setAttribute("aria-selected", String(chapter));
        element("BookTab").tabIndex = chapter ? -1 : 0; element("ChapterTab").tabIndex = chapter ? 0 : -1;
        element("Testaments").hidden = chapter;
        if (chapter) {
            const book = books.find(item => item.name === navigationBook) || books[0];
            const grid = node("div", undefined, "bs-chapter-grid");
            for (let number = 1; number <= book.chapters; number++) {
                const button = node("button", number); button.type = "button";
                button.setAttribute("aria-label", book.name + " " + number);
                if (state.book === book.name && state.chapter === number) button.setAttribute("aria-current", "true");
                button.addEventListener("click", () => navigatePassage(book.name, number, 1,true)); grid.append(button);
            }
            element("ChapterGrid").replaceChildren(node("h3", book.name), grid);
        }
    }
    function navigatePassage(book, chapter, verse=1, wholeChapter=false, endVerse=verse) {
        readerScroll.bible = 0;
        state.book = book; state.chapter = chapter; state.verse = verse;
        for (const name of ["NavigationDialog", "HistoryDialog"]) if (element(name).open) element(name).close();
        updateChapter(); savePreferences();
        if(!wholeChapter)setIllustrationPassage(verse,endVerse);
        if(endVerse>verse){
            const pending=chapterLoadPromise;
            pending.then(()=>{
                if(pending!==chapterLoadPromise)return;
                for(const item of verseData.values())if(item.verse<=endVerse && (item.verse_end || item.verse)>=verse){
                    const target=element("Verse"+item.verse);
                    target?.classList.add("bs-jump-target");
                    setTimeout(()=>target?.classList.remove("bs-jump-target"),2500);
                }
            });
        }
    }
    function drawBookGrid() {
        const panel = element("BookGrid"); panel.replaceChildren();
        element("OldTestament").setAttribute("aria-pressed", String(navigationTestament === "old"));
        element("NewTestament").setAttribute("aria-pressed", String(navigationTestament === "new"));
        for (const [label, start, end] of navigationTestament === "old" ? [["Old Testament", 0, 39]] : [["New Testament", 39, 66]]) {
            const grid = node("div", undefined, "bs-book-group");
            books.slice(start, end).forEach((book, offset) => {
                const index = start + offset;
                const button = node("button", abbreviations[index]); button.type = "button";
                button.title = book.name; button.setAttribute("aria-label", book.name);
                button.dataset.bookGroup = index < 5 ? "law" : index < 17 ? "history" : index < 22 ? "poetry" : index < 39 ? "prophets" : index < 44 ? "gospel" : index < 65 ? "letters" : "apocalypse";
                if (book.name === state.book) button.setAttribute("aria-current", "true");
                button.addEventListener("click", () => { navigationBook = book.name; showNavigationTab(true); element("ChapterTab").focus(); });
                grid.append(button);
            });
            panel.append(node("h3", label), grid);
        }
    }
    element("Navigate").addEventListener("click", () => {
        navigationTestament = books.findIndex(book => book.name === state.book) >= 39 ? "new" : "old";
        navigationBook = state.book; drawBookGrid(); showNavigationTab(false);
        element("NavigationError").textContent = ""; element("ReferenceInput").value = "";
        element("NavigationDialog").showModal();
        element("NavigationDialog").querySelector(".bs-navigation-body").scrollTop = 0;
    });
    element("OldTestament").addEventListener("click", () => { navigationTestament = "old"; drawBookGrid(); showNavigationTab(false); });
    element("NewTestament").addEventListener("click", () => { navigationTestament = "new"; drawBookGrid(); showNavigationTab(false); });
    element("BookTab").addEventListener("click", () => showNavigationTab(false));
    element("ChapterTab").addEventListener("click", () => showNavigationTab(true));
    for (const name of ["BookTab", "ChapterTab"]) element(name).addEventListener("keydown", event => {
        if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
        event.preventDefault(); const chapter = event.key === "End" || event.key !== "Home" && name === "BookTab";
        showNavigationTab(chapter); element(chapter ? "ChapterTab" : "BookTab").focus();
    });
    element("ExpandNavigation").addEventListener("click", () => {
        const expanded = element("NavigationDialog").classList.toggle("bs-expanded");
        element("ExpandNavigation").setAttribute("aria-pressed", String(expanded));
    });
    element("DirectReference").addEventListener("submit", async event => {
        event.preventDefault();
        const value = element("ReferenceInput").value.trim();
        const match = value.match(/^(.+?)\s*(\d+)\s*(?::\s*(\d+)(?:\s*[-–—]\s*(\d+))?)?$/);
        const book=match && books.find(item=>item.name===referenceBookNames.get(normalizeBookName(match[1])));
        const chapter=Number(match?.[2]),verse=Number(match?.[3] || 1),endVerse=Number(match?.[4] || verse);
        if (!book || chapter < 1 || chapter > book.chapters || verse < 1 || endVerse < verse || endVerse > 176) {
            element("NavigationError").textContent = "Enter a valid reference, such as Gen. 1:2-5 or John 3:16."; return;
        }
        element("NavigationError").textContent="";
        if (contentAvailable && match[3]) {
            try {
                const parameters={translation:state.translation,book:book.name,chapter,verse,end_verse:endVerse};
                await api(config.verseUrl+"?"+new URLSearchParams(parameters));
                // The preview API can return a partial passage. Check the final
                // verse too so a range beyond the chapter is not silently cut.
                if(endVerse!==verse)await api(config.verseUrl+"?"+new URLSearchParams({...parameters,verse:endVerse}));
            }
            catch (error) { element("NavigationError").textContent = error.message; return; }
        }
        navigatePassage(book.name,chapter,verse,!match[3],endVerse);
    });
    element("History").addEventListener("click", async () => {
        element("HistoryList").replaceChildren(node("p", "Loading reading history…")); element("HistoryDialog").showModal();
        try {
            const data = await api(apiBase + "/history");
            element("HistoryList").replaceChildren();
            if (!data.history.length) element("HistoryList").append(node("p", "Your visited passages will appear here."));
            for (const item of data.history) {
                const button = node("button", item.book + " " + item.chapter + ":" + item.verse + " · " + (item.translation || "NIV"), "bs-history-item"); button.type = "button";
                button.append(node("small", new Date(item.visited_at).toLocaleString()), node("small", item.snippet));
                button.addEventListener("click", () => { state.translation = item.translation || "NIV"; applyTranslation(); navigatePassage(item.book, item.chapter, item.verse); }); element("HistoryList").append(button);
            }
        } catch (error) { element("HistoryList").replaceChildren(node("p", error.message)); }
    });
    for (const [close, dialog] of [["CloseNavigation", "NavigationDialog"], ["CloseHistory", "HistoryDialog"], ["CloseChain", "ChainDialog"], ["CloseSource", "SourceDialog"]]) {
        element(close).addEventListener("click", () => element(dialog).close());
    }
    element("Smaller").addEventListener("click", () => {
        state.font_size = Math.max(14, state.font_size - 1); applyAppearance(); savePreferences();
    });
    element("Larger").addEventListener("click", () => {
        state.font_size = Math.min(30, state.font_size + 1); applyAppearance(); savePreferences();
    });
    element("Theme").addEventListener("change", event => {
        state.theme = event.target.value; applyAppearance(); savePreferences();
    });
    element("Settings").addEventListener("click", () => {
        window.getSelection()?.removeAllRanges(); clearSelectionActions(); element("SettingsDialog").showModal();
    });
    for (const name of ["Settings", "Saved", "Note", "Copy", "SavedStudy"]) element("Close" + name).addEventListener("click", () => element(name + "Dialog").close());
    element("OpenSaved").addEventListener("click", () => openSavedItems());
    element("FontSize").addEventListener("input", event => { state.font_size = Number(event.target.value); applyAppearance(); });
    element("FontSize").addEventListener("change", savePreferences);
    element("ShowMargins").addEventListener("change", event => { localSettings.margins = event.target.checked; saveLocalSettings(); applyAppearance(); });
    element("LayoutMode").addEventListener("change", event => setMode(event.target.value, true));
    root.querySelectorAll("[data-bs-mode]").forEach(button => button.addEventListener("click", () => {
        if (element("SettingsDialog").open) element("SettingsDialog").close();
        setMode(button.dataset.bsMode, true);
    }));
    document.addEventListener("fullscreenchange", () => {
        if (!document.fullscreenElement && state.layout_mode !== "split") setMode("split", true);
    });
    root.addEventListener("keydown", event => {
        if (event.key === "Escape" && state.layout_mode !== "split" && !root.querySelector("dialog[open]")) {
            event.preventDefault(); setMode("split", true);
        }
    });
    element("CloseVerse").addEventListener("click", () => element("VerseDialog").close());

    const divider = element("Divider");
    let dragging = null;
    divider.addEventListener("pointerdown", event => {
        if (event.button !== 0 || state.layout_mode !== "split") return;
        dragging = event.pointerId; divider.setPointerCapture(event.pointerId); event.preventDefault();
    });
    divider.addEventListener("pointermove", event => {
        if (dragging !== event.pointerId) return;
        const rect = element("Layout").getBoundingClientRect();
        setRatio(mobile.matches ? (event.clientY - rect.top) / rect.height * 100 : (event.clientX - rect.left) / rect.width * 100);
    });
    function finishDrag(event) {
        if (dragging !== event.pointerId) return;
        dragging = null;
        if (divider.hasPointerCapture(event.pointerId)) divider.releasePointerCapture(event.pointerId);
        if (mobile.matches) { localSettings.mobileRatio = state.split_ratio; saveLocalSettings(); }
        savePreferences();
    }
    divider.addEventListener("pointerup", finishDrag);
    divider.addEventListener("pointercancel", finishDrag);
    divider.addEventListener("lostpointercapture", finishDrag);
    divider.addEventListener("keydown", event => {
        const previous = mobile.matches ? "ArrowUp" : "ArrowLeft";
        const next = mobile.matches ? "ArrowDown" : "ArrowRight";
        if (![previous, next, "Home", "End"].includes(event.key)) return;
        event.preventDefault();
        setRatio(event.key === "Home" ? 20 : event.key === "End" ? 80 : state.split_ratio + (event.key === next ? 2 : -2));
        savePreferences();
    });
    mobile.addEventListener("change", () => {positionTabs();setRatio(state.split_ratio);});
    new ResizeObserver(()=>setRatio(state.split_ratio)).observe(element("Layout"));

    (async function initialize() {
        if(element("Sync") && config.syncUrl && new URLSearchParams(location.search).get("sync")==="1"){
            element("SyncDialog").showModal();pollSync();
        }
        const startup= config.startupPreload===true;
        const startupAt=performance.now();
        function progress(value,message){
            if(!startup)return;
            window.iowoStartupProgress={value,message};
            window.IOWOAndroid?.studyProgress?.(value,message);
            window.dispatchEvent(new CustomEvent("iowo-study-progress",{detail:{value,message}}));
        }
        progress(48,"Opening your Bible and saved reading position");
        const notesReady=startup?loadSermonNotes(""):Promise.resolve();
        // Attach rejection handling immediately while the Bible opens in parallel.
        const preparedNotes=notesReady.then(()=>({ok:true}),error=>({ok:false,error}));
        try {
            const [status, preferences] = await Promise.all([api(config.statusUrl), api(config.stateUrl)]);
            storageAvailable = status.schema_ready && preferences.storage_available;
            contentAvailable = status.indexed_verses > 0;
            Object.assign(state, preferences.state);
            element("Status").textContent = status.message;
            element("SaveStatus").textContent = storageAvailable
                ? "Reading preferences can be saved to your account."
                : "Reading preferences will not be saved until Bible Study storage is initialized.";
        } catch (error) {
            element("Status").textContent = error.message;
            element("SaveStatus").textContent = "Reading preferences have not been saved.";
        }
        const query = new URLSearchParams(location.search);
        const linkedBook=books.find(book=>book.name===query.get("book"));
        const linkedChapter=Number(query.get("chapter")),linkedVerse=Number(query.get("verse"));
        if(linkedBook && Number.isInteger(linkedChapter) && linkedChapter>=1 && linkedChapter<=linkedBook.chapters
                && Number.isInteger(linkedVerse) && linkedVerse>=1 && linkedVerse<=176){
            state.book=linkedBook.name;state.chapter=linkedChapter;state.verse=linkedVerse;
            state.translation="NIV";state.layout_mode="split";
        }
        const kind = query.has("annotation") ? "annotations" : query.has("bookmark") ? "bookmarks" : null;
        if (kind) {
            try {
                const data = await api(apiBase + "/personal/" + kind + "/" + encodeURIComponent(query.get(kind.slice(0, -1))));
                if (!data.item.available) throw Error("This saved item belongs to a Bible edition that is currently unavailable.");
                state.translation = data.item.translation || "NIV"; state.book = data.item.book; state.chapter = data.item.chapter; state.verse = data.item.verse;
                if (state.layout_mode === "study") state.layout_mode = "split";
            } catch (error) { toast(error.message); }
        }
        progress(58,"Preparing Bible translations");
        applyTranslation(); await refreshTranslations();
        updateChapter(); applyAppearance(); setRatio(mobile.matches ? (localSettings.mobileRatio || 50) : state.split_ratio); setMode(state.layout_mode);
        if(kind || state.verse>1)setIllustrationPassage(state.verse,state.verse);
        if(query.has("study")) {
            try { const saved=await api(apiBase+"/study-saves/"+encodeURIComponent(query.get("study")));await openSavedStudy(saved.item); }
            catch(error){toast(error.message);}
        }
        if(startup){
            try{
                await chapterLoadPromise;
                progress(70,"Bible ready; preparing your sermon notes");
                const notes=await preparedNotes;
                if(!notes.ok)throw notes.error;
                if(!contentAvailable || !verseData.size)throw Error("The Bible passage could not be prepared. Please reopen the app.");
                progress(80,"Preparing study tools for your current chapter");
                const chapter=new URLSearchParams({book:state.book,chapter:state.chapter});
                const warmups=[
                    ["Book Study",()=>loadBookStudy()],
                    ["Commentary",()=>api(commentaryUrl())],
                    ["Sermon outlines",()=>api(sermonListUrl())],
                    ["Illustrations",()=>api(apiBase+"/illustrations/suggested?"+chapter)],
                    ["Personal marks",()=>loadPersonal()]
                ];
                let completed=0;
                // Independent tools prepare concurrently; failures remain visible
                // in their own panels and cannot hide a healthy Bible/notes library.
                const results=await Promise.allSettled(warmups.map(async([name,run])=>{
                    try{return await run();}finally{progress(80+Math.round(++completed/warmups.length*17),name+" prepared");}
                }));
                window.iowoStartupWarnings=results.flatMap((result,index)=>result.status==="rejected"?[warmups[index][0]]:[]);
                if(document.fonts?.ready)await document.fonts.ready;
                window.iowoStudyReady=true;
                window.iowoStartupDurationMs=Math.round(performance.now()-startupAt);
                progress(100,"Your Bible and sermon notes are ready");
                window.IOWOAndroid?.studyReady?.();
                window.dispatchEvent(new Event("iowo-study-ready"));
            }catch(error){
                window.iowoStartupError=error.message;
                window.IOWOAndroid?.studyFailed?.(error.message);
                element("Status").textContent=error.message;
            }
        }
        if (element("Sync") && config.syncUrl) pollSync();
    })();
})();
