/* Prepare the resource before a separate tap opens the phone's native share menu. */
(function () {
    "use strict";

    window.createResourceFileActions = function (prefix) {
        const element = suffix => document.getElementById(prefix + suffix);
        const overlay = element("Overlay");
        const title = element("Title");
        const name = element("Name");
        const status = element("Status");
        const saveButton = element("Save");
        const shareButton = element("Share");
        const cancelButton = element("Cancel");
        const direct = element("Direct");
        let controller = null;
        let file = null;
        let bookTitle = "";
        let previousFocus = null;
        let sharing = false;

        function setReady(ready) {
            saveButton.disabled = !ready;
            shareButton.disabled = !ready;
        }

        function formatBytes(value) {
            const bytes = Math.max(0, Number(value || 0));
            const units = ["B", "KB", "MB", "GB"];
            const index = bytes ? Math.min(3, Math.floor(Math.log(bytes) / Math.log(1024))) : 0;
            return (bytes / Math.pow(1024, index)).toFixed(index ? 1 : 0) + " " + units[index];
        }

        function updateProgress(loaded, total) {
            const bar = element("Bar");
            const known = total > 0;
            bar.classList.toggle("indeterminate", !known);
            bar.style.width = known ? Math.min(100, loaded / total * 100) + "%" : "";
            element("Bytes").textContent = formatBytes(loaded) + (known ? " / " + formatBytes(total) : " received");
            element("Percent").textContent = known ? Math.min(100, Math.round(loaded / total * 100)) + "%" : "";
        }

        function filenameFromHeader(header, fallback) {
            const encoded = String(header || "").match(/filename\*=UTF-8''([^;]+)/i);
            if (encoded) {
                const value = encoded[1].trim().replace(/^"|"$/g, "");
                try { return decodeURIComponent(value); } catch (_error) { return value; }
            }
            const plain = String(header || "").match(/filename="?([^";]+)"?/i);
            return plain ? plain[1].trim() : fallback;
        }

        function close() {
            if (sharing) return;
            if (controller) controller.abort();
            controller = null;
            file = null;
            setReady(false);
            overlay.classList.remove("show");
            overlay.setAttribute("aria-hidden", "true");
            overlay.setAttribute("aria-busy", "false");
            if (previousFocus?.isConnected) previousFocus.focus();
        }

        async function open(url, label) {
            if (controller || sharing) return;
            previousFocus = document.activeElement;
            file = null;
            bookTitle = label || "ebook";
            setReady(false);
            title.textContent = "Preparing ebook...";
            name.textContent = bookTitle;
            status.textContent = "Once ready, choose Save to device or Share file.";
            cancelButton.textContent = "Cancel";
            direct.href = url;
            direct.classList.remove("show");
            updateProgress(0, 0);
            overlay.classList.add("show");
            overlay.setAttribute("aria-hidden", "false");
            overlay.setAttribute("aria-busy", "true");
            cancelButton.focus();

            const request = new AbortController();
            controller = request;
            try {
                const response = await fetch(url, {
                    credentials: "same-origin", cache: "no-store", signal: request.signal
                });
                if (!response.ok) throw new Error("Unable to prepare the ebook (HTTP " + response.status + ").");
                const type = (response.headers.get("Content-Type") || "application/octet-stream").split(";")[0].trim().toLowerCase();
                if (type === "text/html" || type === "application/json") {
                    throw new Error("The server did not return an ebook. Please sign in again and retry.");
                }
                const filename = filenameFromHeader(response.headers.get("Content-Disposition"), bookTitle);
                const total = Number(response.headers.get("Content-Length") || 0);
                const chunks = [];
                let loaded = 0;
                if (response.body?.getReader) {
                    const reader = response.body.getReader();
                    while (true) {
                        const {done, value} = await reader.read();
                        if (done) break;
                        if (request.signal.aborted || controller !== request) return;
                        if (!value) continue;
                        chunks.push(value);
                        loaded += value.byteLength;
                        updateProgress(loaded, total);
                    }
                } else {
                    const blob = await response.blob();
                    chunks.push(blob);
                }
                if (request.signal.aborted || controller !== request) return;
                const blob = new Blob(chunks, {type});
                if (!blob.size) throw new Error("The ebook file is empty. Please retry.");
                // Keep saving available even in browsers without the File constructor.
                file = typeof File === "function" ? new File([blob], filename, {type}) : blob;
                if (!file.name) file.name = filename;
                name.textContent = filename;
                updateProgress(blob.size, blob.size);
                title.textContent = "Ebook ready";
                status.textContent = "Choose Save to device or Share file. Your phone will show the available apps.";
                cancelButton.textContent = "Close";
                setReady(true);
            } catch (error) {
                if (request.signal.aborted || controller !== request) return;
                title.textContent = "Unable to prepare ebook";
                status.textContent = error.message || "Please retry or use Direct download.";
                cancelButton.textContent = "Close";
                direct.classList.add("show");
            } finally {
                if (controller === request) {
                    controller = null;
                    overlay.setAttribute("aria-busy", "false");
                }
            }
        }

        function save() {
            if (!file || sharing) return;
            const objectUrl = URL.createObjectURL(file);
            const link = document.createElement("a");
            link.href = objectUrl;
            link.download = file.name;
            link.style.display = "none";
            document.body.appendChild(link);
            link.click();
            link.remove();
            // Give the browser time to start saving before releasing its URL.
            setTimeout(() => URL.revokeObjectURL(objectUrl), 60000);
            status.textContent = "Download started. You can also share this file.";
        }

        async function share() {
            if (!file || sharing) return;
            const selectedFile = file;
            try {
                if (!navigator.share || !navigator.canShare || !navigator.canShare({files: [selectedFile]})) {
                    status.textContent = "This browser cannot share this file. Choose Save to device, then share it from your files.";
                    return;
                }
                sharing = true;
                setReady(false);
                cancelButton.disabled = true;
                // No fetch or await before this call: preserve the tap's user activation.
                await navigator.share({title: bookTitle, files: [selectedFile]});
                status.textContent = "File handed to your phone's share menu.";
            } catch (error) {
                status.textContent = error?.name === "AbortError"
                    ? "Sharing cancelled. You can try again or save the file."
                    : "Unable to share this file. Try again or choose Save to device.";
            } finally {
                sharing = false;
                cancelButton.disabled = false;
                setReady(Boolean(file));
            }
        }

        saveButton.addEventListener("click", save);
        shareButton.addEventListener("click", share);
        overlay.addEventListener("click", event => { if (event.target === overlay) close(); });
        document.addEventListener("keydown", event => {
            if (!overlay.classList.contains("show")) return;
            if (event.key === "Escape") {
                event.preventDefault();
                event.stopImmediatePropagation();
                close();
            } else if (event.key === "Tab") {
                const buttons = Array.from(overlay.querySelectorAll("button, a[href]"))
                    .filter(button => !button.disabled && button.getClientRects().length);
                const first = buttons[0];
                const last = buttons[buttons.length - 1];
                if (event.shiftKey && document.activeElement === first) {
                    event.preventDefault();
                    last?.focus();
                } else if (!event.shiftKey && document.activeElement === last) {
                    event.preventDefault();
                    first?.focus();
                }
            }
        }, true);
        window.addEventListener("pagehide", () => { if (controller) controller.abort(); file = null; });
        return {open, close};
    };
})();
