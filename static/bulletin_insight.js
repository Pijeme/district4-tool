(function () {
    "use strict";
    const node = (tag, text, className) => {
        const element = document.createElement(tag);
        if (text !== undefined) element.textContent = text;
        if (className) element.className = className;
        return element;
    };
    async function api(url, csrf, body) {
        const options = {credentials: "same-origin", cache: "no-store"};
        if (body) Object.assign(options, {method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": csrf}, body: JSON.stringify(body)});
        const response = await fetch(url, options);
        const data = await response.json().catch(() => ({}));
        if (!response.ok || !data.ok) throw Error(data.error || "Unable to update. Please try again.");
        return data;
    }
    const stamp = value => new Date(value).toLocaleString("en-PH", {timeZone: "Asia/Manila", dateStyle: "medium", timeStyle: "short"});

    function setupDevotion() {
        const configElement = document.getElementById("maxwellInsightConfig"), card = document.getElementById("maxwellInsight");
        if (!configElement || !card || window.districtDevotionInstalled) return;
        window.districtDevotionInstalled = true;
        const config = JSON.parse(configElement.textContent);
        const actions = document.getElementById("devotionActions");
        const el = name => document.getElementById("devotion" + name);
        const dialog = el("Dialog");
        let insight = config.insight, liked = false, kind = "", next = null, revision = 0;
        function wireDetails() {
            const details = card.querySelector("details");
            if (details) details.addEventListener("toggle", () => {
                details.querySelector("summary").textContent = details.open ? "Read less" : "Read more";
            });
        }
        function counts(data) {
            liked = data.liked;
            for (const [name, label] of [["Likes", "likes"], ["Comments", "comments"], ["Shares", "shares"]]) el(name).textContent = data[label] + " " + label;
            el("LikeLabel").textContent = liked ? "Liked" : "Like";
            el("Like").setAttribute("aria-pressed", String(liked));
        }
        async function refresh() {
            if (!insight || !config.interactions || document.visibilityState === "hidden") return;
            try { counts(await api(config.interactions)); }
            catch (error) { el("Status").textContent = error.message; }
        }
        async function loadPeople(append = false) {
            const epoch = ++revision, selected = kind;
            const query = new URLSearchParams({kind: selected});
            if (append && next !== null) query.set(selected === "comments" ? "before" : "offset", next);
            el("DialogStatus").textContent = "Loading...";
            el("More").disabled = true;
            try {
                const data = await api(config.interactions + "?" + query);
                if (epoch !== revision || !dialog.open) return;
                counts(data);
                if (!append) el("People").replaceChildren();
                const items = selected === "comments" ? data.entries : data.people;
                for (const item of items) {
                    const row = node("article", undefined, "devotion-person");
                    row.append(node("strong", item.actor_name), node("p", item.church_name || "District 4", "muted small"));
                    if (selected === "comments") {
                        row.append(node("p", item.content, "devotion-comment-text"));
                        if (item.can_delete) {
                            const remove = node("button", "Delete comment"); remove.type = "button";
                            remove.addEventListener("click", async () => {
                                remove.disabled = true;
                                try { await api(config.interactions, config.csrf, {action: "delete_comment", id: item.id}); await loadPeople(); }
                                catch (error) { el("DialogStatus").textContent = error.message; remove.disabled = false; }
                            });
                            row.append(remove);
                        }
                    } else if (selected === "shares") row.append(node("p", item.method === "copy_message" ? "Copied message" : "Used share menu", "muted small"));
                    row.append(node("time", stamp(item.created_at), "muted small"));
                    el("People").append(row);
                }
                next = selected === "comments" ? data.next_before : data.next_offset;
                el("More").hidden = next === null;
                el("DialogStatus").textContent = items.length ? "" : append ? "No more entries." : "No " + selected + " yet.";
            } catch (error) { if (epoch === revision) el("DialogStatus").textContent = error.message; }
            finally { if (epoch === revision) el("More").disabled = false; }
        }
        function openPeople(selected) {
            kind = selected; next = null;
            el("DialogTitle").textContent = selected === "likes" ? "People who liked this" : selected === "shares" ? "People who shared this" : "Comments - District-wide";
            el("CommentForm").hidden = selected !== "comments";
            if (!dialog.open) dialog.showModal();
            loadPeople();
        }
        if (actions && dialog) {
            for (const [name, selected] of [["Likes", "likes"], ["Shares", "shares"], ["Comments", "comments"], ["Comment", "comments"]]) el(name).addEventListener("click", () => openPeople(selected));
            el("Close").addEventListener("click", () => dialog.close());
            dialog.addEventListener("close", () => { ++revision; });
            el("More").addEventListener("click", () => loadPeople(true));
            el("Like").addEventListener("click", async () => {
                el("Like").disabled = true;
                try { counts(await api(config.interactions, config.csrf, {action: "like", liked: !liked})); el("Status").textContent = ""; }
                catch (error) { el("Status").textContent = error.message; }
                finally { el("Like").disabled = false; }
            });
            el("CommentForm").addEventListener("submit", async event => {
                event.preventDefault();
                const field = el("CommentText"), button = event.target.querySelector("button");
                if (!field.value.trim()) return;
                button.disabled = true;
                try {
                    counts(await api(config.interactions, config.csrf, {action: "comment", content: field.value.trim()}));
                    field.value = ""; await loadPeople();
                } catch (error) { el("DialogStatus").textContent = error.message; }
                finally { button.disabled = false; }
            });
            function message() {
                return ["Daily Devotional - " + config.day, insight.title, insight.content,
                    "Verse of the Day", config.verse, config.reference + " (NIV)", "Source: " + (insight.source_title || "Maxwell Leadership Bible")].join("\n\n");
            }
            async function share() {
                if (!insight || el("Share").disabled) return;
                el("Share").disabled = true;
                el("Status").textContent = "";
                try {
                    if (!window.isSecureContext) {
                        el("Status").textContent = "Open this website using HTTPS to use your phone's share menu. Sharing is unavailable on a local HTTP connection.";
                        return;
                    }
                    const data = {title: insight.title, text: message()};
                    if (!navigator.share || (navigator.canShare && !navigator.canShare(data))) {
                        el("Status").textContent = "This browser cannot open a share menu for this message. Try a supported mobile browser, such as Chrome or Safari.";
                        return;
                    }
                    // Like PDF sharing, call directly from the tap before any fetch or await.
                    await navigator.share(data);
                    el("Status").textContent = "Message handed to your phone's share menu. Delivery is handled by your chosen app.";
                } catch (error) {
                    el("Status").textContent = error.name === "AbortError" ? "Sharing cancelled." : "Unable to open the share menu. Please try again.";
                    return;
                } finally { el("Share").disabled = false; }
                try { counts(await api(config.interactions, config.csrf, {action: "share", method: "share_menu"})); }
                catch (_error) { el("Status").textContent = "Share menu opened, but the activity could not be recorded. Refresh to update the counts."; }
            }
            el("Share").addEventListener("click", share);
            document.addEventListener("visibilitychange", refresh);
            window.addEventListener("focus", refresh);
        }
        wireDetails();
        if (actions) refresh();
        async function poll(attempt = 0) {
            try {
                const data = await api(config.url);
                if (data.reference !== config.reference || data.day !== config.day) return;
                if (data.insight) {
                    insight = data.insight;
                    const details = document.createElement("details");
                    details.append(node("summary", "Read more"), node("div", insight.content, "maxwell-source-content"));
                    card.replaceChildren(node("h3", "Daily Devotional"), node("p", "From the Maxwell Leadership Bible", "muted small"),
                        node("strong", insight.title), node("p", insight.passage, "muted small"), node("p", insight.excerpt), details);
                    wireDetails(); card.hidden = false; card.style.removeProperty("display");
                    if (actions) { actions.hidden = false; refresh(); }
                } else if (data.pending && attempt < 40) setTimeout(() => poll(attempt + 1), 2000);
            } catch (_error) {}
        }
        if (config.pending) poll();
    }

    function setupReminder() {
        const configElement = document.getElementById("pastorReminderConfig"), dialog = document.getElementById("pastorReportReminder");
        if (!configElement || !dialog || window.districtReminderInstalled) return;
        window.districtReminderInstalled = true;
        const config = JSON.parse(configElement.textContent);
        document.getElementById("pastorReportReminderClose").addEventListener("click", () => dialog.close());
        let checking = false;
        async function checkReminder() {
            if (checking || document.visibilityState === "hidden") return;
            if (document.querySelector("dialog[open]") || document.documentElement.classList.contains("district-page-loading")) {
                setTimeout(checkReminder, 1000); return;
            }
            checking = true;
            try {
                const data = await api(config.url, config.csrf, {});
                if (!data.show) return;
                document.getElementById("pastorReportReminderChurch").textContent = data.church;
                const list = document.getElementById("pastorReportReminderList"); list.replaceChildren();
                for (const report of data.reports) {
                    const item = node("li"), link = node("a", report.label + (report.overdue ? " - Overdue" : " - Not submitted"));
                    link.href = report.url; item.append(link); list.append(item);
                }
                document.getElementById("pastorReportReminderOpen").href = data.reports[0].url;
                dialog.showModal();
            } catch (_error) { /* Reporting remains accessible when reminders are unavailable. */ }
            finally { checking = false; }
        }
        checkReminder();
        window.addEventListener("pageshow", checkReminder);
        document.addEventListener("visibilitychange", checkReminder);
    }
    setupDevotion();
    setupReminder();
})();
