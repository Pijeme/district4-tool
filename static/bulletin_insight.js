(function () {
    "use strict";
    const configElement = document.getElementById("maxwellInsightConfig");
    const card = document.getElementById("maxwellInsight");
    if (!configElement || !card) return;
    const config = JSON.parse(configElement.textContent);
    function node(tag, text, className) {
        const element = document.createElement(tag);
        element.textContent = text;
        if (className) element.className = className;
        return element;
    }
    const existingDetails = card.querySelector("details");
    if (existingDetails) existingDetails.addEventListener("toggle", () => {
        existingDetails.querySelector("summary").textContent = existingDetails.open ? "Read less" : "Read more";
    });
    async function poll(attempt = 0) {
        try {
            const response = await fetch(config.url, {credentials: "same-origin", cache: "no-store"});
            if (!response.ok) return;
            const data = await response.json();
            if (data.reference !== config.reference || data.day !== config.day) return;
            if (data.insight) {
                const insight = data.insight;
                const details = document.createElement("details");
                const summary = node("summary", "Read more");
                details.append(summary, node("div", insight.content, "maxwell-source-content"));
                details.addEventListener("toggle", () => { summary.textContent = details.open ? "Read less" : "Read more"; });
                card.replaceChildren(node("h3", "Leadership Insight"),
                    node("p", "Maxwell Leadership Bible", "muted small"),
                    node("strong", insight.title), node("p", insight.passage, "muted small"),
                    node("p", insight.summary), details);
                card.hidden = false;
                card.style.removeProperty("display");
            } else if (data.pending && attempt < 40) {
                setTimeout(() => poll(attempt + 1), 2000);
            }
            // No matching note or a generation failure leaves the area invisible.
        } catch (_error) {}
    }
    if (config.pending) poll();
})();
