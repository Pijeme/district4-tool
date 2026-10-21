/* SQLite reader and developer sync controls. Source markup is never rendered. */
(function () {
    "use strict";
    const root = document.getElementById("bibleStudyRoot");
    if (!root) return;
    const config = JSON.parse(document.getElementById("bsConfig").textContent);
    const element = name => document.getElementById("bs" + name);
    const books = [...element("Book").options].map(option => ({name: option.value, chapters: Number(option.dataset.chapters)}));
    const mobile = window.matchMedia("(max-width: 700px)");
    const state = {book: "Genesis", chapter: 1, verse: 1, theme: "paper", font_size: 19, layout_mode: "split", split_ratio: 60, version: 0};
    let storageAvailable = false, saveChain = Promise.resolve(), writePaused = false, previousFocus = null;
    let contentAvailable = false, chapterRequest = 0, wordRequest = 0, verseRequest = 0;
    let copiedVerse = "", syncTimer = null, syncBusy = false;
    let navigationBook = state.book, chainRequest = 0;
    let studyTab = "word", selectedWord = null, concordanceRequest = 0, currentChain = null;
    let navigationTestament = "old", readerTab = "bible", bookStudyRequest = 0, bookStudyBook = "";
    const readerScroll = {bible: 0, book: 0};
    let readerPassageChanged = false;
    let commentaryRequest = 0, commentarySeries = "wiersbe";
    const commentaryNames = {wiersbe: "Wiersbe · Be Series", "exalting-jesus": "Exalting Jesus"};
    function sizeWorkspace() {
        if (!root.classList.contains("bs-fullscreen")) root.style.setProperty("--bs-page-top", Math.ceil(root.getBoundingClientRect().top + window.scrollY) + "px");
    }
    window.addEventListener("resize", sizeWorkspace);
    if (document.querySelector(".app-header")) new ResizeObserver(sizeWorkspace).observe(document.querySelector(".app-header"));
    sizeWorkspace();
    const readerChromeHeight = () => root.querySelector(".bs-reader-tabs").getBoundingClientRect().height + root.querySelector(".bs-toolbar").getBoundingClientRect().height;
    const readerTabSize = new ResizeObserver(() => root.style.setProperty("--bs-reader-tab-height", root.querySelector(".bs-reader-tabs").offsetHeight + "px"));
    readerTabSize.observe(root.querySelector(".bs-reader-tabs"));
    const chainHistory = [];
    let scrollTimer = null, scrollRestoreUntil = 0;
    const apiBase = config.statusUrl.replace(/\/status$/, "");

    function node(tag, text, className) {
        const item = document.createElement(tag);
        if (text !== undefined) item.textContent = text;
        if (className) item.className = className;
        return item;
    }

    async function api(url, options = {}) {
        const response = await fetch(url, {credentials: "same-origin", cache: "no-store", ...options});
        if (!(response.headers.get("Content-Type") || "").includes("application/json")) {
            throw Error("Please sign in again and reopen Bible Study.");
        }
        const data = await response.json();
        if (!response.ok || !data.ok) throw Error(data.error || "Bible Study is temporarily unavailable.");
        return data;
    }

    async function loadChapter() {
        const requestId = ++chapterRequest;
        element("ReaderError").hidden = true;
        ++wordRequest;
        ++concordanceRequest; selectedWord = null;
        element("Verses").replaceChildren();
        element("WordResults").replaceChildren();
        element("ConcordanceResults").replaceChildren(node("p", "Select a Bible word or search the concordance."));
        element("Empty").hidden = false;
        if (!contentAvailable || !config.chapterUrl) return;
        try {
            const query = new URLSearchParams({book: state.book, chapter: state.chapter});
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
                if (verse.heading && verse.heading !== lastHeading) {
                    element("Verses").append(node("h3", verse.heading)); lastHeading = verse.heading;
                }
                const row = node("div", undefined, (verse.topics || []).length ? "bs-verse-row" : "");
                const paragraph = node("p", undefined, "bs-verse" + (["Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Songs"].includes(state.book) ? " bs-poetry" : ""));
                paragraph.id = "bsVerse" + verse.verse;
                paragraph.append(node("sup", verse.verse + " ", "bs-verse-number"));
                const characters = Array.from(verse.text);
                let offset = 0;
                for (const token of verse.tokens) {
                    appendScriptureText(paragraph, characters, offset, token.start_offset, verse.red_spans || []);
                    const button = node("button", token.text, "bs-word"); button.type = "button";
                    if ((verse.red_spans || []).some(span => token.start_offset < span.end_offset && token.end_offset > span.start_offset)) button.classList.add("bs-red-letter");
                    button.addEventListener("click", () => {
                        root.querySelectorAll(".bs-word.is-selected").forEach(item => item.classList.remove("is-selected"));
                        button.classList.add("is-selected"); state.verse = verse.verse;
                        savePreferences();
                        selectedWord = {word: token.text, verseId: verse.id, tokenIndex: token.token_index};
                        ++wordRequest; ++concordanceRequest;
                        element("Search").value = element("ConcordanceSearch").value = token.text;
                        if (studyTab === "concordance") loadConcordance(token.text);
                        else if (studyTab === "commentary") loadCommentary();
                        else loadWord(verse.id, token.token_index);
                    });
                    paragraph.append(button); offset = token.end_offset;
                }
                appendScriptureText(paragraph, characters, offset, characters.length, verse.red_spans || []);
                row.append(paragraph);
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
            if (verses.length && storageAvailable) {
                const target = verses.find(verse => verse.verse === state.verse) || verses[0];
                api(apiBase + "/history", {method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": config.csrfToken}, body: JSON.stringify({verse_id: target.id})}).catch(() => {});
                scrollRestoreUntil = performance.now() + 500;
                if (readerTab !== "bible") return;
                if (state.verse > 1) {
                    const targetElement = element("Verse" + state.verse);
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

    async function loadWord(verseId, tokenIndex) {
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

    function renderSourceBlock(block) {
        const paragraph = node(block.role === "commentary_heading" && block.level > 1 ? "h6" : block.type === "heading" ? "h5" : "p");
        if (block.role === "major_chain") paragraph.classList.add("bs-major-chain");
        if (["commentary_main_idea", "commentary_outline", "commentary_outline_continuation", "commentary_heading", "commentary_reference", "commentary_section"].includes(block.role)) {
            paragraph.classList.add("bs-" + block.role.replaceAll("_", "-"));
            if ([1, 2, 3].includes(block.level)) paragraph.classList.add("bs-commentary-level-" + block.level);
            if (block.marker) paragraph.append(node("span", block.marker + " ", "bs-commentary-marker"));
        }
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
        const links = [...(block.references || []), ...(block.chain_links || []).map(link => ({...link, chain: true}))].sort((a, b) => a.start - b.start);
        for (const reference of links) {
            if (reference.start < offset || reference.end > position) continue;
            appendSlice(paragraph, offset, reference.start);
            const button = node("button", undefined, reference.chain ? "bs-chain-ref" : "bs-scripture-ref"); button.type = "button";
            appendSlice(button, reference.start, reference.end);
            button.addEventListener("click", () => reference.chain
                ? previewChain(reference.topic_id, true, reference.key) : previewVerse(reference));
            paragraph.append(button); offset = reference.end;
        }
        appendSlice(paragraph, offset, position);
        return paragraph;
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
        selectedWord = {word: query}; element("ConcordanceSearch").value = query;
        ++concordanceRequest;
        const requestId = ++wordRequest;
        element("WordResults").replaceChildren(node("p", "Searching Mounce and Vine…"));
        try {
            const data = await api(apiBase + "/dictionaries/search?" + new URLSearchParams({q: query}));
            if (requestId === wordRequest) { renderDictionaryResults(data, requestId); renderPeople(data.people, requestId); }
        } catch (error) {
            if (requestId === wordRequest) element("WordResults").replaceChildren(node("p", error.message));
        }
    });

    function selectStudyTab(name, focus = false) {
        if (!["word", "concordance", "commentary"].includes(name)) return;
        const changed = studyTab !== name;
        if (changed) { ++wordRequest; ++concordanceRequest; ++commentaryRequest; }
        studyTab = name;
        for (const [tab, prefix] of [["word", "Word"], ["concordance", "Concordance"], ["commentary", "Commentary"]]) {
            const active = tab === name;
            element(prefix + "Tab").setAttribute("aria-selected", String(active));
            element(prefix + "Tab").tabIndex = active ? 0 : -1;
            element(prefix + "Panel").hidden = !active;
        }
        if (focus) element(name === "word" ? "WordTab" : name === "concordance" ? "ConcordanceTab" : "CommentaryTab").focus();
        if (name === "commentary") { loadCommentary(); return; }
        if (!changed || !selectedWord) return;
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
            const tabs = ["word", "concordance", "commentary"];
            const next = (tabs.indexOf(studyTab) + (event.key === "ArrowLeft" ? -1 : 1) + tabs.length) % tabs.length;
            selectStudyTab(event.key === "Home" ? tabs[0] : event.key === "End" ? tabs[2] : tabs[next], true);
        });
    });

    async function loadCommentary() {
        const requestId = ++commentaryRequest;
        const results = element("CommentaryResults");
        const query = new URLSearchParams({book: state.book, chapter: state.chapter, verse: state.verse, series: commentarySeries});
        results.replaceChildren(node("p", "Loading passage commentary…"));
        try {
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
                let outline = null;
                for (const block of entry.blocks) {
                    if (["commentary_outline", "commentary_outline_continuation"].includes(block.role)) {
                        if (!outline) {
                            outline = node("section", undefined, "bs-commentary-outline-card");
                            outline.setAttribute("aria-label", "Commentary outline");
                            outline.append(node("h5", "Outline")); content.append(outline);
                        }
                        outline.append(renderSourceBlock(block));
                    } else { outline = null; content.append(renderSourceBlock(block)); }
                }
                article.append(content, node("p", entry.author + " · " + entry.source_title, "bs-attribution"));
                results.append(article);
            }
        } catch (error) {
            if (requestId === commentaryRequest) results.replaceChildren(node("p", error.message));
        }
    }

    root.querySelectorAll("[data-commentary-series]").forEach(button => button.addEventListener("click", () => {
        const series = button.dataset.commentarySeries;
        if (!commentaryNames[series] || series === commentarySeries) return;
        commentarySeries = series;
        root.querySelectorAll("[data-commentary-series]").forEach(item => item.setAttribute("aria-pressed", String(item === button)));
        if (studyTab === "commentary") loadCommentary();
    }));

    async function loadConcordance(query, offset = 0, ownerRequest = null) {
        const requestId = offset === 0 ? ++concordanceRequest : ownerRequest;
        const results = element("ConcordanceResults");
        if (offset === 0) results.replaceChildren(node("p", "Searching Thompson concordance…"));
        try {
            const data = await api(apiBase + "/concordance?" + new URLSearchParams({q: query, offset}));
            if (requestId !== concordanceRequest) return;
            if (offset === 0) results.replaceChildren(node("h3", data.query));
            if (!data.available) { results.append(node("p", "The Thompson concordance has not been synced yet.")); return; }
            if (!data.entries.length && offset === 0) results.append(node("p", "No entry for this word is listed in the Thompson concordance."));
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
                card.append(content, node("p", "Thompson concordance · PDF page " + entry.source_page, "bs-attribution"));
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

    async function loadBookStudy() {
        if (bookStudyBook === state.book) return;
        const requestId = ++bookStudyRequest, book = state.book;
        element("BookStudyTitle").textContent = book;
        element("BookStudyResults").replaceChildren(node("p", "Loading Thompson book study…"));
        try {
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
        const requestId = ++verseRequest;
        copiedVerse = ""; element("CopyVerse").disabled = true;
        element("CopyVerse").textContent = "Copy Verse";
        element("VerseReference").textContent = reference.book + " " + reference.chapter + ":" + reference.verse;
        element("VerseText").textContent = "Loading verse…"; element("VerseAttribution").textContent = "";
        if (!element("VerseDialog").open) element("VerseDialog").showModal();
        try {
            const data = await api(config.verseUrl + "?" + new URLSearchParams({book: reference.book, chapter: reference.chapter, verse: reference.verse,
                end_chapter: reference.end_chapter || reference.chapter, end_verse: reference.end_verse || reference.verse}));
            if (requestId !== verseRequest) return;
            element("VerseReference").textContent = data.verse.reference;
            element("VerseText").textContent = data.verse.text;
            element("VerseAttribution").textContent = data.verse.attribution;
            copiedVerse = data.verse.reference + " (NIV)\n" + data.verse.text;
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
    element("VerseDialog").addEventListener("close", () => { ++verseRequest; });
    root.querySelectorAll("dialog").forEach(dialog => {
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
        try {
            await navigator.clipboard.writeText(copiedVerse);
            element("CopyVerse").textContent = "Copied";
        } catch (_error) {
            element("CopyVerse").textContent = "Select the verse text to copy";
        }
    });

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
                const status = await api(config.statusUrl);
                contentAvailable = status.indexed_verses > 0;
                storageAvailable = status.schema_ready;
                element("Status").textContent = status.message;
                element("SaveStatus").textContent = "Reading preferences can be saved to your account.";
                loadChapter();
            }
        } catch (error) {
            syncBusy = false; element("Sync").disabled = false;
            element("Sync").textContent = "Sync Bible Study";
            element("SyncStatus").textContent = error.message;
            element("SyncDetails").textContent = "Sync details · Error";
        }
    }

    if (element("Sync") && config.syncUrl) {
        element("SyncDetails").addEventListener("click", () => element("SyncDialog").showModal());
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
            if (readerTab === "book") { element("Bible").scrollTop = 0; loadBookStudy(); }
        }
        loadChapter();
        ++commentaryRequest;
        if (studyTab === "commentary") loadCommentary();
    }

    function applyAppearance() {
        root.dataset.theme = state.theme;
        root.style.setProperty("--bs-font", state.font_size + "px");
        element("Theme").value = state.theme;
        element("Smaller").disabled = state.font_size <= 14;
        element("Larger").disabled = state.font_size >= 30;
    }

    function setRatio(value) {
        state.split_ratio = Math.max(20, Math.min(80, Math.round(value)));
        root.style.setProperty("--bs-split", state.split_ratio);
        element("Divider").setAttribute("aria-valuenow", state.split_ratio);
        element("Divider").setAttribute("aria-orientation", mobile.matches ? "horizontal" : "vertical");
    }

    async function setMode(mode, userAction = false) {
        if (!["split", "bible", "study"].includes(mode)) mode = "split";
        state.layout_mode = mode;
        root.dataset.mode = mode;
        root.classList.toggle("bs-fullscreen", mode !== "split");
        root.querySelectorAll("[data-bs-mode]").forEach(button => button.setAttribute("aria-pressed", String(button.dataset.bsMode === mode)));
        if (mode === "split") {
            if (document.fullscreenElement === root) {
                try { await document.exitFullscreen(); } catch (_error) {}
            }
            if (userAction) previousFocus?.focus?.();
        } else if (userAction) {
            previousFocus = document.activeElement;
            // Keep a full-window CSS mode if the browser has no Fullscreen API.
            if (!document.fullscreenElement && root.requestFullscreen) {
                try { await root.requestFullscreen(); } catch (_error) {}
            }
            element(mode === "bible" ? "Bible" : "Study").focus();
        }
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
    let swipe = null, suppressWordClickUntil = 0;
    const scripture = element("Scripture");
    scripture.addEventListener("pointerdown", event => {
        if (!event.isPrimary) { swipe = null; return; }
        if (!mobile.matches || event.pointerType !== "touch" || readerTab !== "bible"
                || root.querySelector("dialog[open]") || window.getSelection()?.toString()) return;
        const control = event.target.closest("button,a,input,select,textarea,summary");
        if (control && !control.classList.contains("bs-word")) return;
        swipe = {id: event.pointerId, x: event.clientX, y: event.clientY, time: performance.now()};
    }, {passive: true});
    scripture.addEventListener("pointermove", event => {
        if (swipe?.id !== event.pointerId) return;
        const dx = Math.abs(event.clientX - swipe.x), dy = Math.abs(event.clientY - swipe.y);
        if (dy > 20 && dy > dx) swipe = null;
    }, {passive: true});
    scripture.addEventListener("pointercancel", () => { swipe = null; });
    scripture.addEventListener("pointerup", event => {
        const gesture = swipe; swipe = null;
        if (!gesture || gesture.id !== event.pointerId || performance.now() - gesture.time > 700
                || window.getSelection()?.toString() || root.querySelector("dialog[open]")) return;
        const dx = event.clientX - gesture.x, dy = event.clientY - gesture.y;
        if (Math.abs(dx) < 70 || Math.abs(dx) < Math.abs(dy) * 2) return;
        suppressWordClickUntil = performance.now() + 400;
        stepChapter(dx < 0 ? 1 : -1);
    }, {passive: true});
    scripture.addEventListener("click", event => {
        if (performance.now() < suppressWordClickUntil) { event.preventDefault(); event.stopImmediatePropagation(); }
    }, true);

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
            try { await api(config.verseUrl + "?" + new URLSearchParams({book: book.name, chapter: match[2], verse: match[3]})); }
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
                const button = node("button", item.book + " " + item.chapter + ":" + item.verse, "bs-history-item"); button.type = "button";
                button.append(node("small", new Date(item.visited_at).toLocaleString()), node("small", item.snippet));
                button.addEventListener("click", () => navigatePassage(item.book, item.chapter, item.verse)); element("HistoryList").append(button);
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
    root.querySelectorAll("[data-bs-mode]").forEach(button => button.addEventListener("click", () => setMode(button.dataset.bsMode, true)));
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
    mobile.addEventListener("change", () => setRatio(state.split_ratio));

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
        updateChapter(); applyAppearance(); setRatio(state.split_ratio); setMode(state.layout_mode);
        if (element("Sync") && config.syncUrl) pollSync();
    })();
})();
