/* Owner-only activity dashboard and change notification popup. */
(() => {
    "use strict";
    const configElement = document.getElementById("websiteActivityConfig");
    if (!configElement) return;
    const config = JSON.parse(configElement.textContent);
    const root = document.getElementById("activityUpdates");
    const popup = document.getElementById("websiteUpdatesPopup");
    const stamp = value => new Date(value).toLocaleString("en-PH", {timeZone: "Asia/Manila", dateStyle: "medium", timeStyle: "short"});
    const node = (tag, text, className) => { const e = document.createElement(tag); if (text !== undefined) e.textContent = text; if (className) e.className = className; return e; };
    async function api(url, options = {}) {
        const response = await fetch(url, {credentials: "same-origin", cache: "no-store", ...options});
        const data = await response.json().catch(() => ({}));
        if (!response.ok || !data.ok) throw Error(data.error || "Unable to load activity. Please refresh and try again.");
        return data;
    }
    const post = (url, body) => api(url, {method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": config.csrf}, body: JSON.stringify(body)});
    const storageKey = "website-updates-" + config.csrf;
    let noticed = 0, checking = false;
    try { noticed = Number(sessionStorage.getItem(storageKey) || 0); } catch (_) {}
    async function checkUpdates() {
        if (root || checking || document.visibilityState === "hidden") return;
        checking = true;
        try {
            const data = await api(config.api + "?view=summary");
            if (data.unread && data.latest_id > noticed) {
                document.getElementById("websiteUpdatesMessage").textContent = `You have ${data.unread} unread change notification${data.unread === 1 ? "" : "s"}.`;
                if (!popup.open) popup.showModal();
                noticed = data.latest_id;
                try { sessionStorage.setItem(storageKey, String(noticed)); } catch (_) {}
            }
        } catch (_) { /* A notification check must not interrupt normal use. */ }
        finally { checking = false; }
    }
    document.getElementById("websiteUpdatesDismiss").addEventListener("click", () => popup.close());
    window.addEventListener("pageshow", checkUpdates);
    document.addEventListener("visibilitychange", checkUpdates);
    checkUpdates();
    if (!root) return;
    const element = name => document.getElementById("au" + name);
    let view = new URLSearchParams(location.search).get("tab") === "notifications" ? "notifications" : "accounts", page = 1, pages = 1, revision = 0;
    function params(kind, requestedPage) {
        return new URLSearchParams({view: kind, page: requestedPage, q: element("Search").value, start: element("Start").value, end: element("End").value});
    }
    function eventCard(item) {
        const card = node("article", undefined, "au-event" + (item.is_notification && !item.read_at ? " au-unread" : ""));
        card.append(node("time", stamp(item.created_at)), node("p", item.description));
        card.append(node("p", `${item.actor_name || "Guest"}${item.actor_username ? " · " + item.actor_username : ""} · ${item.actor_role}`, "au-meta"));
        if (item.page_path) card.append(node("p", item.page_path, "au-meta"));
        if (item.details && Object.keys(item.details).length) {
            const details = node("details"); details.append(node("summary", "Change details"), node("pre", JSON.stringify(item.details, null, 2))); card.append(details);
        }
        return card;
    }
    function accountCard(account) {
        const details = node("details", undefined, "au-account"), summary = node("summary");
        summary.append(node("span", "Expand activities", "au-meta"), node("strong", `${account.actor_name || "Guest"}${account.actor_username ? " · " + account.actor_username : ""}`), node("span", `${account.actor_role} · ${account.activity_count} activities · ${stamp(account.last_activity)}`, "au-meta"));
        const history = node("div", undefined, "au-history");
        details.append(summary, history);
        let loaded = false, busy = false, activityPage = 1;
        async function loadHistory() {
            if (busy) return; busy = true; const epoch = revision;
            try {
                const query = params("activities", activityPage); query.set("account", account.actor_key);
                const data = await api(config.api + "?" + query);
                if (epoch !== revision || !details.isConnected) return;
                if (activityPage === 1) history.replaceChildren();
                history.querySelector("button")?.remove();
                data.items.forEach(item => history.append(eventCard(item))); loaded = true;
                if (!data.items.length) history.append(node("p", "No activities in this period."));
                if (data.page < data.pages) {
                    const more = node("button", "Load more activities", "btn-secondary"); more.type = "button";
                    more.onclick = () => { activityPage++; loadHistory(); }; history.append(more);
                }
            } catch (error) { history.append(node("p", error.message)); }
            finally { busy = false; }
        }
        details.addEventListener("toggle", () => { if (details.open && !loaded) loadHistory(); });
        return details;
    }
    async function load() {
        const epoch = ++revision;
        element("Status").textContent = "Loading…";
        element("AccountsTab").setAttribute("aria-selected", String(view === "accounts"));
        element("NotificationsTab").setAttribute("aria-selected", String(view === "notifications"));
        element("AccountsTab").className = view === "accounts" ? "btn-primary" : "btn-secondary";
        element("NotificationsTab").className = view === "notifications" ? "btn-primary" : "btn-secondary";
        element("Results").setAttribute("aria-labelledby", view === "accounts" ? "auAccountsTab" : "auNotificationsTab");
        try {
            const data = await api(config.api + "?" + params(view, page));
            if (epoch !== revision) return;
            pages = data.pages;
            element("Results").replaceChildren(...data.items.map(view === "accounts" ? accountCard : eventCard));
            element("Status").textContent = data.total ? `${data.total} ${view === "accounts" ? "accounts" : "notifications"}` : "No records yet.";
            element("Page").textContent = `Page ${page} of ${pages}`;
            element("Prev").disabled = page <= 1; element("Next").disabled = page >= pages;
            if (view === "notifications" && data.items.length) {
                await post(config.read, {ids: data.items.map(item => item.id)});
            }
        } catch (error) { if (epoch === revision) element("Status").textContent = error.message; }
    }
    element("AccountsTab").onclick = () => { view = "accounts"; page = 1; load(); };
    element("NotificationsTab").onclick = () => { view = "notifications"; page = 1; load(); };
    element("Filters").onsubmit = event => { event.preventDefault(); page = 1; load(); };
    element("Prev").onclick = () => { if (page > 1) { page--; load(); } };
    element("Next").onclick = () => { if (page < pages) { page++; load(); } };
    element("Clear").onclick = () => element("ClearDialog").showModal();
    element("ClearCancel").onclick = () => element("ClearDialog").close();
    element("ClearConfirm").onclick = async () => {
        element("ClearConfirm").disabled = true;
        try { await post(config.clear, {}); element("ClearDialog").close(); page = 1; await load(); }
        catch (error) { element("Status").textContent = error.message; }
        finally { element("ClearConfirm").disabled = false; }
    };
    load();
})();
