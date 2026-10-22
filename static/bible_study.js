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
    let lastVerseReference = null;
    let copiedVerse = "", syncTimer = null, syncBusy = false;
    let navigationBook = state.book, chainRequest = 0;
    let studyTab = "word", selectedWord = null, concordanceRequest = 0, currentChain = null;
    let navigationTestament = "old", readerTab = "bible", bookStudyRequest = 0, bookStudyBook = "";
    const readerScroll = {bible: 0, book: 0};
    let readerPassageChanged = false;
    let commentaryRequest = 0, commentarySeries = "wiersbe", pulpitSection = "exposition", greekRequest = 0;
    const commentaryNames = {wiersbe: "Wiersbe · Be Series", "exalting-jesus": "Exalting Jesus", moody: "The Moody Bible Commentary", pulpit: "The Pulpit Commentary"};
    function sizeWorkspace() {
        if(document.body.classList.contains("bible-study-page")) {
            // Size the app shell, rather than measuring a root whose position
            // can change during Safari's initial scroll restoration or zoom.
            const viewport=window.visualViewport;
            if(!viewport || Math.abs(viewport.scale-1)<.01) {
                document.body.style.setProperty("--bs-viewport-height",Math.round(viewport?.height || window.innerHeight)+"px");
            }
            return;
        }
        const top = root.getBoundingClientRect().top;
        root.style.setProperty("--bs-page-top", Math.ceil(top) + "px");
        root.style.setProperty("--bs-workspace-height", Math.max(140, (window.visualViewport?.height || window.innerHeight) - top) + "px");
    }
    window.addEventListener("resize", sizeWorkspace);
    window.visualViewport?.addEventListener("resize", sizeWorkspace);
    if (document.querySelector(".app-header")) new ResizeObserver(sizeWorkspace).observe(document.querySelector(".app-header"));
    window.addEventListener("pageshow",sizeWorkspace);
    sizeWorkspace();
    const readerTabs=root.querySelector(".bs-reader-tabs"),studyTabs=element("Study").querySelector(".bs-study-tabs");
    function positionTabs() {
        if(mobile.matches) {
            element("Bible").prepend(readerTabs);element("Study").prepend(studyTabs);
            element("EditionLabel").after(element("TranslationControl"));
        } else {
            element("WideTabs").append(readerTabs,studyTabs);
            element("History").before(element("TranslationControl"));
        }
        root.style.setProperty("--bs-reader-tab-height",(mobile.matches?readerTabs.offsetHeight:0)+"px");
    }
    positionTabs();
    const readerChromeHeight = () => mobile.matches?readerTabs.getBoundingClientRect().height:0;
    const readerTabSize = new ResizeObserver(() => root.style.setProperty("--bs-reader-tab-height", readerChromeHeight() + "px"));
    readerTabSize.observe(root.querySelector(".bs-reader-tabs"));
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
    function commentaryUrl(){return apiBase+"/commentary?"+new URLSearchParams({book:state.book,chapter:state.chapter,verse:state.verse,series:commentarySeries,section:pulpitSection});}
    function sermonListUrl(){return apiBase+"/sermons?"+new URLSearchParams({book:state.book,chapter:state.chapter});}
    function scheduleBackgroundStudy(){
        cancelBackgroundStudy();const epoch=backgroundEpoch;
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

    async function api(url, options = {}) {
        const key=String(url), cacheable=(!options.method || options.method==="GET") &&
            /\/(?:commentary\?|sermons(?:\?|\/)|word\/|concordance\?|dictionaries\/(?:entry\/|search\?))/.test(key);
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
                    if(!response.ok || !result.ok)throw Error(result.error || "Bible Study is temporarily unavailable.");
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
        element("WordTab").disabled = state.translation !== "NIV";
        element("WordTab").title = state.translation === "NIV" ? "" : "English word study is available with NIV. Use Concordance for this translation.";
        if (state.translation !== "NIV" && studyTab === "word") selectStudyTab("concordance");
    }
    element("Translation").addEventListener("change", () => {
        state.translation = element("Translation").value;
        ++wordRequest; ++concordanceRequest; ++verseRequest; selectedWord = null;
        applyTranslation(); updateChapter(); savePreferences();
        if (element("VerseDialog").open && lastVerseReference) previewVerse(lastVerseReference);
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
        element("Empty").hidden = false;
        if (!contentAvailable || !config.chapterUrl) return;
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
            let lastHeading = "";
            for (const verse of verses) {
                verseData.set(verse.id, verse);
                if (verse.heading && verse.heading !== lastHeading) {
                    element("Verses").append(node("h3", verse.heading)); lastHeading = verse.heading;
                }
                const row = node("div", undefined, (verse.topics || []).length ? "bs-verse-row" : "");
                const paragraph = node("p", undefined, "bs-verse" + (verse.layout === "poetry" || ["Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Songs"].includes(state.book) ? " bs-poetry" : ""));
                paragraph.id = "bsVerse" + verse.verse;
                paragraph.dataset.verseId = verse.id;
                const number = node("button", verse.verse_end > verse.verse ? verse.verse + "–" + verse.verse_end : verse.verse, "bs-verse-number"); number.type = "button";
                number.setAttribute("aria-label", "Actions for " + state.book + " " + state.chapter + ":" + verse.verse);
                number.addEventListener("click", () => selectWholeVerse(verse)); paragraph.append(number);
                const textContainer = node("span", undefined, "bs-verse-text"); paragraph.append(textContainer);
                const characters = Array.from(verse.text);
                let offset = 0;
                for (const token of verse.tokens) {
                    appendScriptureText(textContainer, characters, offset, token.start_offset, verse.red_spans || []);
                    const button = node("span", token.text, "bs-word");
                    button.setAttribute("role", "button"); button.tabIndex = 0;
                    if ((verse.red_spans || []).some(span => token.start_offset < span.end_offset && token.end_offset > span.start_offset)) button.classList.add("bs-red-letter");
                    button.addEventListener("click", () => {
                        if (window.getSelection()?.toString() || performance.now() < suppressWordClickUntil) return;
                        clearSelectionActions();
                        root.querySelectorAll(".bs-word.is-selected").forEach(item => item.classList.remove("is-selected"));
                        button.classList.add("is-selected"); state.verse = verse.verse;
                        savePreferences();
                        selectedWord = {word: token.text, verseId: verse.id, tokenIndex: token.token_index};
                        ++wordRequest; ++concordanceRequest;
                        element("Search").value = element("ConcordanceSearch").value = token.text;
                        if (studyTab === "concordance") loadConcordance(token.text);
                        else if (studyTab === "commentary") loadCommentary();
                        else if (studyTab === "word" && state.translation === "NIV") loadWord(verse.id, token.token_index);
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
                row.append(paragraph);
                if (verse.footnotes?.length) {
                    const notes = node("details", undefined, "bs-verse-footnotes");
                    notes.append(node("summary", "Notes · " + verse.verse));
                    for (const note of verse.footnotes) notes.append(node("p", note.text));
                    row.append(notes);
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
        if (selectionBusy || [...root.querySelectorAll("dialog[open]")].some(dialog=>!["bsChainDialog","bsVerseDialog","bsSavedStudyDialog","bsGreekDialog"].includes(dialog.id))) return;
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
    async function copyScripture(text) {
        if (navigator.clipboard?.writeText && window.isSecureContext) {
            try { await navigator.clipboard.writeText(text); toast("Scripture copied with its reference."); return; } catch (_error) {}
        }
        // Local Wi-Fi HTTP does not expose the Clipboard API on mobile browsers.
        const fallback = document.createElement("textarea"); fallback.value = text;
        fallback.style.cssText = "position:fixed;left:0;top:0;opacity:0;font-size:16px"; root.append(fallback);
        fallback.focus(); fallback.select(); fallback.setSelectionRange(0, text.length);
        let copied = false;
        try { copied = document.execCommand("copy"); } catch (_error) {}
        fallback.remove();
        if (copied) toast("Scripture copied with its reference.");
        else {
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

    function loadWord(verseId,tokenIndex){return visibleStudy(()=>fetchWord(verseId,tokenIndex));}
    async function fetchWord(verseId, tokenIndex) {
        const requestId = ++wordRequest;
        const results = element("WordResults");
        results.replaceChildren(node("p", "Looking up this word…"));
        try {
            const data = await api(apiBase + "/word/" + verseId + "/" + tokenIndex);
            if (requestId !== wordRequest) return;
            element("Search").value = data.word;
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
            results.insertBefore(original, results.children[1] || null);
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

    function renderSourceBlock(block, sourceBlocks=null) {
        const paragraph = node(block.role === "commentary_heading" && block.level > 1 ? "h6" : block.type === "heading" ? "h5" : "p");
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
                } else {
                    const characters = Array.from(item.run.text);
                    content = document.createTextNode(characters.slice(Math.max(0, from - item.start), Math.min(characters.length, to - item.start)).join(""));
                }
                for (const mark of (item.run.marks || [])) {
                    if (!["em", "strong", "sup", "sub"].includes(mark)) continue;
                    const wrapper = document.createElement(mark); wrapper.append(content); content = wrapper;
                }
                parent.append(content);
            }
        }
        let offset = 0;
        const links = [...(block.references || []), ...(block.chain_links || []).map(link => ({...link, chain: true})),
            ...(block.greek_links || []).map(link=>({...link,greek:true}))].sort((a, b) => a.start - b.start);
        for (const reference of links) {
            if (reference.start < offset || reference.end > position) continue;
            appendSlice(sourceText, offset, reference.start);
            const button = node("button", undefined, reference.greek ? "bs-greek-ref" : reference.chain ? "bs-chain-ref" : "bs-scripture-ref"); button.type = "button";
            appendSlice(button, reference.start, reference.end);
            let pressedAt=0;
            button.addEventListener("pointerdown",()=>{pressedAt=performance.now();});
            button.addEventListener("click", event => {
                if(reference.greek && (performance.now()<suppressWordClickUntil || window.getSelection()?.toString().trim()
                    || (event.detail && pressedAt && performance.now()-pressedAt>450)))return;
                if(reference.greek)previewGreek(reference,block,sourceBlocks);
                else if(reference.chapter_only)navigatePassage(reference.book,reference.chapter,1);
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
        const card = node("article", undefined, "bs-entry");
        card.append(node("h4", entry.headword), node("p", [entry.section, entry.language].filter(Boolean).join(" · "), "bs-attribution"));
        if (entry.evidence?.length) card.append(node("p", entry.evidence.join(" · "), "bs-evidence"));
        if (entry.identifiers?.length) card.append(node("p", entry.identifiers.filter(item => item.scheme === "strong").map(item => (item.language === "Hebrew" ? "H" : "G") + item.number + item.qualifier).join(" · "), "bs-attribution"));
        const preview = renderSourceBlock(entry.preview_block || {type: "paragraph", text: entry.preview, references: entry.preview_references || []});
        card.append(preview);
        const full = node("div", undefined, "bs-source-content"); full.hidden = true; card.append(full);
        const more = node("button", "Show more", "bs-show-more"); more.type = "button";
        more.setAttribute("aria-expanded", "false"); card.append(more);
        let loaded = false;
        more.addEventListener("click", async () => {
            if (!full.hidden) {
                full.hidden = true; preview.hidden = false; more.textContent = "Show more"; more.setAttribute("aria-expanded", "false"); return;
            }
            more.disabled = true;
            try {
                if (!loaded) {
                    const data = await api(apiBase + "/dictionaries/entry/" + entry.id + (entry.sense_id ? "?sense=" + entry.sense_id : ""));
                    if (ownerRequest !== wordRequest) return;
                    const source = data.entry;
                    full.append(node("p", source.source_title, "bs-attribution"));
                    if (source.identifiers.length) {
                        const identifiers = source.identifiers.map(identifier => (identifier.scheme === "strong"
                            ? (identifier.language === "Hebrew" ? "H" : "G") : "GK " + identifier.language + " ")
                            + identifier.number + identifier.qualifier);
                        full.append(node("p", [...new Set(identifiers)].join(" · "), "bs-attribution"));
                    }
                    for (const block of source.blocks) full.append(renderSourceBlock(block));
                    if (!source.blocks.length) full.append(node("p", "This source lists the heading without a separate definition."));
                    loaded = true;
                }
                full.hidden = false; preview.hidden = true; more.textContent = "Show less"; more.setAttribute("aria-expanded", "true");
            } catch (error) {
                full.replaceChildren(node("p", error.message)); full.hidden = false;
            } finally {
                more.disabled = false;
            }
        });
        return card;
    }

    function renderDictionaryResults(data, ownerRequest) {
        const results = element("WordResults");
        results.replaceChildren(node("h3", data.query || data.word));
        for (const group of data.sources) {
            const section = node("section", undefined, "bs-dictionary");
            const toggle = node("button", group.label, "bs-source-toggle"); toggle.type = "button"; toggle.setAttribute("aria-expanded", "true");
            const body = node("div");
            toggle.addEventListener("click", () => { body.hidden = !body.hidden; toggle.setAttribute("aria-expanded", String(!body.hidden)); });
            section.append(toggle, body); results.append(section);
            if (!group.available) body.append(node("p", "This dictionary has not been synced yet."));
            else if (!group.entries.length) body.append(node("p", "No matching entry found in this book."));
            for (const entry of group.entries) body.append(dictionaryEntryCard(entry, ownerRequest));
            if (group.next_offset !== null) {
                const next = node("button", "More matching entries"); next.type = "button"; body.append(next);
                let offset = group.next_offset;
                next.addEventListener("click", async () => {
                    next.disabled = true;
                    try {
                        const query = new URLSearchParams({q: data.query, source: group.source, offset});
                        const page = await api(apiBase + "/dictionaries/search?" + query);
                        if (ownerRequest !== wordRequest) return;
                        for (const entry of page.sources[0].entries) body.insertBefore(dictionaryEntryCard(entry, ownerRequest), next);
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
        const finish=beginStudyLoad();
        selectedWord = {word: query}; element("ConcordanceSearch").value = query;
        ++concordanceRequest;
        const requestId = ++wordRequest;
        element("WordResults").replaceChildren(node("p", "Searching Mounce and Vine…"));
        try {
            const data = await api(apiBase + "/dictionaries/search?" + new URLSearchParams({q: query}));
            if (requestId === wordRequest) { renderDictionaryResults(data, requestId); renderPeople(data.people, requestId); }
        } catch (error) {
            if (requestId === wordRequest) element("WordResults").replaceChildren(node("p", error.message));
        } finally{finish();}
    });

    function selectStudyTab(name, focus = false) {
        if (!["word", "concordance", "commentary", "sermon"].includes(name)) return;
        if (name === "word" && state.translation !== "NIV") name = "concordance";
        const changed = studyTab !== name;
        if (changed) { ++wordRequest; ++concordanceRequest; ++commentaryRequest; }
        studyTab = name;
        cancelBackgroundStudy();
        if(["commentary","sermon"].includes(name))preferredStudy=name;
        for (const [tab, prefix] of [["word", "Word"], ["concordance", "Concordance"], ["commentary", "Commentary"], ["sermon", "Sermon"]]) {
            const active = tab === name;
            element(prefix + "Tab").setAttribute("aria-selected", String(active));
            element(prefix + "Tab").tabIndex = active ? 0 : -1;
            element(prefix + "Panel").hidden = !active;
        }
        if (focus) element(({word:"Word",concordance:"Concordance",commentary:"Commentary",sermon:"Sermon"})[name]+"Tab").focus();
        if (name === "sermon") { loadSermons(); return; }
        if (name === "commentary") { loadCommentary(); return; }
        if (!changed || !selectedWord) {scheduleBackgroundStudy();return;}
        element("Search").value = element("ConcordanceSearch").value = selectedWord.word;
        if (name === "concordance") loadConcordance(selectedWord.word);
        else if (selectedWord.verseId !== undefined) loadWord(selectedWord.verseId, selectedWord.tokenIndex);
        else element("SearchForm").requestSubmit();
    }
    root.querySelectorAll("[data-study-tab]").forEach(button => {
        button.addEventListener("click", () => selectStudyTab(button.dataset.studyTab));
        button.addEventListener("keydown", event => {
            if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
            event.preventDefault();
            const tabs = ["word", "concordance", "commentary", "sermon"];
            const next = (tabs.indexOf(studyTab) + (event.key === "ArrowLeft" ? -1 : 1) + tabs.length) % tabs.length;
            selectStudyTab(event.key === "Home" ? tabs[0] : event.key === "End" ? tabs.at(-1) : tabs[next], true);
        });
    });

    let sermonListScroll=0;
    function loadSermons(){return visibleStudy(fetchSermons);}
    async function fetchSermons() {
        const chapter=state.book+" "+state.chapter;
        if(sermonChapter===chapter)return;
        const requestId=++sermonRequest;
        element("SermonReference").textContent=chapter;
        element("SermonReader").hidden=true;element("SermonResults").hidden=false;
        element("SermonResults").replaceChildren(node("p","Finding indexed sermons for this chapter…"));
        try {
            await chapterLoadPromise;
            if(requestId!==sermonRequest || studyTab!=="sermon")return;
            const data=await api(apiBase+"/sermons?"+new URLSearchParams({book:state.book,chapter:state.chapter}));
            if(requestId!==sermonRequest)return;
            sermonChapter=chapter;element("SermonResults").replaceChildren();
            if(!data.sermons?.length)element("SermonResults").append(node("p","No indexed sermon or teaching outline was found for this chapter."));
            for(const sermon of data.sermons||[]) {
                const button=node("button",sermon.title,"bs-sermon-title");button.type="button";
                button.addEventListener("click",()=>openSermon(sermon.id));element("SermonResults").append(button);
            }
        } catch(error){if(requestId===sermonRequest)element("SermonResults").replaceChildren(node("p",error.message));}
    }
    function openSermon(id){return visibleStudy(()=>fetchSermon(id));}
    async function fetchSermon(id) {
        const requestId=++sermonRequest;
        sermonListScroll=element("Study").scrollTop;
        element("SermonResults").hidden=true;element("SermonReader").hidden=false;
        element("SermonTitle").textContent="Loading sermon…";element("SermonMetadata").textContent="";element("SermonContent").replaceChildren();
        try {
            const data=await api(apiBase+"/sermons/"+encodeURIComponent(id));
            if(requestId!==sermonRequest)return;
            const sermon=data.sermon;element("SermonTitle").textContent=sermon.title;
            const passages=(sermon.ranges||[]).map(span=>books[span.book_id-1].name+" "+span.chapter+":"+span.verse+(span.end_chapter!==span.chapter?"–"+span.end_chapter+":"+span.end_verse:span.end_verse!==span.verse?"–"+span.end_verse:""));
            element("SermonMetadata").textContent=[sermon.author,sermon.source_title,sermon.publisher,sermon.purpose,passages.join("; "),"Source page/section "+sermon.source_section].filter(Boolean).join(" · ");
            element("SermonContent").replaceChildren();appendSourceBlocks(element("SermonContent"),sermon.blocks);
            element("Study").scrollTop=0;
        } catch(error){if(requestId===sermonRequest)element("SermonTitle").textContent=error.message;}
    }
    element("SermonBack").addEventListener("click",()=>{
        ++sermonRequest;element("SermonReader").hidden=true;element("SermonResults").hidden=false;element("Study").scrollTop=sermonListScroll;
    });

    function loadCommentary(){return visibleStudy(fetchCommentary);}
    async function fetchCommentary() {
        const renderedKey=[state.book,state.chapter,state.verse,commentarySeries,pulpitSection].join("|");
        if(commentaryRenderedKey===renderedKey && Date.now()-commentaryRenderedAt<sourceCacheLifetime)return;
        commentaryRenderedKey="";
        const requestId = ++commentaryRequest;
        const results = element("CommentaryResults");
        const query = new URLSearchParams({book: state.book, chapter: state.chapter, verse: state.verse, series: commentarySeries, section:pulpitSection});
        results.replaceChildren(node("p", "Loading passage commentary…"));
        try {
            await chapterLoadPromise;
            if(requestId!==commentaryRequest || studyTab!=="commentary")return;
            const data = await api(apiBase + "/commentary?" + query);
            if (requestId !== commentaryRequest || studyTab !== "commentary") return;
            results.replaceChildren(node("h3", data.reference));
            if (!data.available) results.append(node("p", commentaryNames[commentarySeries] + " is not available in the study library yet."));
            else if (!data.entries.length) results.append(node("p", "No indexed " + commentaryNames[commentarySeries] + " section was found for this passage."));
            for (const entry of data.entries) {
                const article = node("article", undefined, "bs-commentary-entry");
                article.append(node("p", entry.source_title, "bs-commentary-volume"), node("h4", entry.title),
                    node("p", "Passage covered: " + entry.passage, "bs-commentary-range"));
                const content = node("div", undefined, "bs-source-content bs-commentary-text");
                if (["moody","pulpit"].includes(commentarySeries)) content.classList.add("bs-source-lines");
                let outline = null;
                for (const block of entry.blocks) {
                    if (commentarySeries === "exalting-jesus" && ["commentary_outline", "commentary_outline_continuation"].includes(block.role)) {
                        if (!outline) {
                            outline = node("section", undefined, "bs-commentary-outline-card");
                            outline.setAttribute("aria-label", "Commentary outline");
                            outline.append(node("h5", "Outline")); content.append(outline);
                        }
                        outline.append(renderSourceBlock(block));
                    } else { outline = null; content.append(renderSourceBlock(block,entry.blocks)); }
                }
                article.append(content, node("p", entry.author + " · " + entry.source_title, "bs-attribution"));
                results.append(article);
            }
            commentaryRenderedKey=renderedKey;commentaryRenderedAt=Date.now();
        } catch (error) {
            if (requestId === commentaryRequest) results.replaceChildren(node("p", error.message));
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
        if (offset === 0) results.replaceChildren(node("p", state.translation === "NIV" ? "Searching Thompson concordance…" : "Searching word matches…"));
        try {
            const data = await api(apiBase + "/concordance?" + new URLSearchParams({q: query, offset, translation: state.translation}));
            if (requestId !== concordanceRequest) return;
            if (offset === 0) results.replaceChildren(node("h3", data.query));
            if (!data.available) { results.append(node("p", state.translation === "NIV" ? "The Thompson concordance has not been synced yet." : "This translation has not been imported yet.")); return; }
            if (!data.entries.length && offset === 0) results.append(node("p", state.translation === "NIV" ? "No entry for this word is listed in the Thompson concordance." : "No verses contain this exact word in the selected translation."));
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

    function renderPeople(data, ownerRequest) {
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
        element("WordResults").insertBefore(card, element("WordResults").children[1] || null);
    }

    function loadBookStudy(){return visibleStudy(fetchBookStudy);}
    async function fetchBookStudy() {
        if (bookStudyBook === state.book) return;
        const requestId = ++bookStudyRequest, book = state.book;
        element("BookStudyTitle").textContent = book;
        element("BookStudyResults").replaceChildren(node("p", "Loading Thompson book study…"));
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
    function selectReaderTab(name, focus = false) {
        if (!["bible", "book"].includes(name) || name === readerTab) return;
        window.getSelection()?.removeAllRanges(); clearSelectionActions();
        readerScroll[readerTab] = element("Bible").scrollTop;
        readerTab = name; scrollRestoreUntil = performance.now() + 500;
        element("ReaderPanel").hidden = name !== "bible"; element("BookStudyPanel").hidden = name !== "book";
        for (const [tab, prefix] of [["bible", "Reader"], ["book", "BookStudy"]]) {
            element(prefix + "Tab").setAttribute("aria-selected", String(name === tab));
            element(prefix + "Tab").tabIndex = name === tab ? 0 : -1;
        }
        element("Bible").scrollTop = readerScroll[name];
        if (name === "bible" && readerPassageChanged) {
            readerPassageChanged = false;
            const target = element("Verse" + state.verse);
            if (target && state.verse > 1) element("Bible").scrollTop += target.getBoundingClientRect().top - element("Bible").getBoundingClientRect().top - readerChromeHeight() - 20;
        }
        if (name === "book") loadBookStudy();
        if (focus) element(name === "bible" ? "ReaderTab" : "BookStudyTab").focus();
    }
    root.querySelectorAll("[data-reader-tab]").forEach(button => {
        button.addEventListener("click", () => selectReaderTab(button.dataset.readerTab));
        button.addEventListener("keydown", event => {
            if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
            event.preventDefault(); selectReaderTab(event.key === "Home" ? "bible" : event.key === "End" ? "book" : readerTab === "bible" ? "book" : "bible", true);
        });
    });

    async function previewVerse(reference) {
        lastVerseReference = reference;
        const requestId = ++verseRequest;
        copiedVerse = ""; element("CopyVerse").disabled = true;
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
        const body = element("SyncReportBody"); body.replaceChildren();
        for (const [key, value] of Object.entries(report.counts || {})) body.append(node("p", key.replaceAll("_", " ") + ": " + Number(value).toLocaleString()));
        for (const warning of report.warnings || []) body.append(node("p", warning));
        element("SyncReport").hidden = !body.childNodes.length;
    }

    async function pollSync() {
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
            syncBusy = false; element("Sync").disabled = false;
            element("Sync").textContent = "Sync Bible Study";
            element("SyncStatus").textContent = error.message;
            element("SyncDetails").textContent = "Sync details · Error";
        }
    }

    if (element("Sync") && config.syncUrl) {
        element("SyncDetails").addEventListener("click", () => {
            if (element("SettingsDialog").open) element("SettingsDialog").close();
            element("SyncDialog").showModal();
        });
        element("CloseSync").addEventListener("click", () => element("SyncDialog").close());
        element("Sync").addEventListener("click", async () => {
            if (syncBusy) return;
            syncBusy = true; element("Sync").disabled = true;
            element("Sync").textContent = "Syncing Bible Study…";
            element("SyncDetails").textContent = "Sync details · Running";
            element("SyncStatus").textContent = "Starting Bible Study sync…";
            try {
                await api(config.syncUrl, {method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": config.csrfToken}, body: "{}"});
                pollSync();
            } catch (error) {
                syncBusy = false; element("Sync").disabled = false;
                element("Sync").textContent = "Sync Bible Study";
                element("SyncStatus").textContent = error.message;
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
        if (sermonChapter !== state.book+" "+state.chapter) { ++sermonRequest; sermonChapter=""; }
        const passageReady=chapterLoadPromise=loadChapter();
        if(readerTab==="book")passageReady.then(()=>{if(passageReady===chapterLoadPromise&&readerTab==="book"&&bookStudyBook!==state.book)loadBookStudy();});
        if (studyTab === "commentary") loadCommentary();
        if (studyTab === "sermon") loadSermons();
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
        sizeWorkspace();
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
        if(event.target.closest(".bs-scripture-ref,.bs-chain-ref")&&(window.getSelection()?.toString()||(pendingSelection?.study&&performance.now()<suppressWordClickUntil))){event.preventDefault();event.stopImmediatePropagation();}
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
                button.addEventListener("click", () => navigatePassage(book.name, number, 1)); grid.append(button);
            }
            element("ChapterGrid").replaceChildren(node("h3", book.name), grid);
        }
    }
    function navigatePassage(book, chapter, verse) {
        readerScroll.bible = 0;
        state.book = book; state.chapter = chapter; state.verse = verse;
        for (const name of ["NavigationDialog", "HistoryDialog"]) if (element(name).open) element(name).close();
        updateChapter(); savePreferences();
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
        root.querySelector(".bs-navigation-body").scrollTop = 0;
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
        const match = value.match(/^(.+?)\s*(\d+)\s*(?::\s*(\d+))?$/);
        let book = null;
        if (match) book = books.find((item, index) => [item.name, abbreviations[index], ...(item.name === "Psalms" ? ["Psalm"] : item.name === "Song of Songs" ? ["Song of Solomon"] : [])].some(name => name.toLowerCase().replace(/[\s.]/g, "") === match[1].toLowerCase().replace(/[\s.]/g, "")));
        if (!book || Number(match[2]) < 1 || Number(match[2]) > book.chapters || Number(match[3] || 1) < 1 || Number(match[3] || 1) > 176) {
            element("NavigationError").textContent = "Enter a valid reference, such as John 3:16."; return;
        }
        if (contentAvailable && match[3]) {
            try { await api(config.verseUrl + "?" + new URLSearchParams({translation: state.translation, book: book.name, chapter: match[2], verse: match[3]})); }
            catch (error) { element("NavigationError").textContent = error.message; return; }
        }
        navigatePassage(book.name, Number(match[2]), Number(match[3] || 1));
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
        const kind = query.has("annotation") ? "annotations" : query.has("bookmark") ? "bookmarks" : null;
        if (kind) {
            try {
                const data = await api(apiBase + "/personal/" + kind + "/" + encodeURIComponent(query.get(kind.slice(0, -1))));
                if (!data.item.available) throw Error("This saved item belongs to a Bible edition that is currently unavailable.");
                state.translation = data.item.translation || "NIV"; state.book = data.item.book; state.chapter = data.item.chapter; state.verse = data.item.verse;
                if (state.layout_mode === "study") state.layout_mode = "split";
            } catch (error) { toast(error.message); }
        }
        applyTranslation(); await refreshTranslations();
        updateChapter(); applyAppearance(); setRatio(mobile.matches ? (localSettings.mobileRatio || 50) : state.split_ratio); setMode(state.layout_mode);
        if(query.has("study")) {
            try { const saved=await api(apiBase+"/study-saves/"+encodeURIComponent(query.get("study")));await openSavedStudy(saved.item); }
            catch(error){toast(error.message);}
        }
        if (element("Sync") && config.syncUrl) pollSync();
    })();
})();
