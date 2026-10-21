/* Visual zoom preserves the EPUB layout, reading location and annotation DOM. */
(function () {
    "use strict";

    window.createEpubPageView = function ({viewport, stage, onChange, selectionActive}) {
        const installed = new WeakSet();
        const frameDocuments = new Map();
        let scale = 1, x = 0, y = 0, gesture = null, blockedUntil = 0;

        function updateTouchStyles() {
            for (const [doc, originals] of frameDocuments) {
                if (!doc.defaultView?.frameElement?.isConnected) {
                    frameDocuments.delete(doc);
                    continue;
                }
                for (const {node, value, priority} of originals) {
                    if (Math.abs(scale - 1) > 0.001) node.style.setProperty("touch-action", "none", "important");
                    else if (value) node.style.setProperty("touch-action", value, priority);
                    else node.style.removeProperty("touch-action");
                }
            }
        }

        function bounds() {
            const width = viewport.clientWidth, height = viewport.clientHeight;
            x = scale < 1 ? width * (1 - scale) / 2 : Math.max(width * (1 - scale), Math.min(0, x));
            y = scale < 1 ? height * (1 - scale) / 2 : Math.max(height * (1 - scale), Math.min(0, y));
        }

        function render() {
            bounds();
            stage.style.transform = `translate(${x}px, ${y}px) scale(${scale})`;
            viewport.classList.toggle("epub-page-zoomed", Math.abs(scale - 1) > 0.001);
            updateTouchStyles();
            onChange?.({scale, interacting: Boolean(gesture)});
        }

        function point(clientX, clientY, target) {
            const doc = target.nodeType === 9 ? target : target.ownerDocument;
            const frame = doc?.defaultView?.frameElement;
            if (frame) {
                const rect = frame.getBoundingClientRect();
                clientX = rect.left + clientX * rect.width / (frame.offsetWidth || rect.width);
                clientY = rect.top + clientY * rect.height / (frame.offsetHeight || rect.height);
            }
            const rect = viewport.getBoundingClientRect();
            return {x: clientX - rect.left, y: clientY - rect.top};
        }

        function zoom(next, focus = {x: viewport.clientWidth / 2, y: viewport.clientHeight / 2}) {
            const value = Math.min(4, Math.max(0.5, Number(next) || 1));
            x = focus.x - (focus.x - x) * value / scale;
            y = focus.y - (focus.y - y) * value / scale;
            scale = value;
            blockedUntil = Date.now() + 450;
            render();
        }

        function stop(event) {
            if (event.cancelable) event.preventDefault();
            event.stopImmediatePropagation();
        }

        function finish(event) {
            if (!gesture) return;
            const handled = gesture.type !== "pan" || gesture.moved;
            gesture = null;
            if (handled) {
                blockedUntil = Date.now() + 450;
                stop(event);
            }
            render();
        }

        function touchPair(event, target) {
            const a = event.touches[0], b = event.touches[1];
            const first = point(a.clientX, a.clientY, target);
            const second = point(b.clientX, b.clientY, target);
            return {
                distance: Math.hypot(second.x - first.x, second.y - first.y),
                center: {x: (first.x + second.x) / 2, y: (first.y + second.y) / 2}
            };
        }

        function install(target) {
            if (!target || installed.has(target)) return;
            installed.add(target);
            if (target.nodeType === 9 && target.defaultView?.frameElement) {
                frameDocuments.set(target, [target.documentElement, target.body].filter(Boolean).map(node => ({
                    node, value: node.style.getPropertyValue("touch-action"),
                    priority: node.style.getPropertyPriority("touch-action")
                })));
                updateTouchStyles();
            }
            const options = {passive: false, capture: true};
            target.addEventListener("touchstart", event => {
                if (event.touches.length === 2) {
                    const pair = touchPair(event, target);
                    gesture = {
                        type: "pinch", distance: pair.distance || 1, scale,
                        contentX: (pair.center.x - x) / scale,
                        contentY: (pair.center.y - y) / scale
                    };
                    blockedUntil = Date.now() + 450;
                    stop(event);
                } else if (event.touches.length === 1 && scale > 1 && !selectionActive?.()) {
                    const touch = event.touches[0];
                    gesture = {type: "pan", start: point(touch.clientX, touch.clientY, target), x, y, moved: false};
                }
            }, options);

            target.addEventListener("touchmove", event => {
                if (gesture?.type === "pinch" && event.touches.length === 2) {
                    const pair = touchPair(event, target);
                    scale = Math.min(4, Math.max(0.5, gesture.scale * pair.distance / gesture.distance));
                    x = pair.center.x - gesture.contentX * scale;
                    y = pair.center.y - gesture.contentY * scale;
                    stop(event);
                    render();
                } else if (gesture?.type === "pan" && event.touches.length === 1) {
                    if (selectionActive?.()) { gesture = null; return; }
                    const touch = event.touches[0], current = point(touch.clientX, touch.clientY, target);
                    const dx = current.x - gesture.start.x, dy = current.y - gesture.start.y;
                    if (!gesture.moved && Math.hypot(dx, dy) < 6) return;
                    gesture.moved = true;
                    x = gesture.x + dx; y = gesture.y + dy;
                    stop(event);
                    render();
                }
            }, options);

            target.addEventListener("touchend", event => {
                if (!gesture) return;
                if (gesture.type === "pinch" && event.touches.length) {
                    // Do not turn a pinch's remaining finger into a swipe.
                    gesture.type = "ending";
                    stop(event);
                } else if (!event.touches.length) finish(event);
            }, options);
            target.addEventListener("touchcancel", finish, options);

            target.addEventListener("mousedown", event => {
                // Shift-drag remains available for selecting text when zoomed.
                if (event.button || event.shiftKey || scale <= 1 || selectionActive?.()) return;
                if (event.target.closest?.("a,button,input,textarea,select")) return;
                gesture = {type: "mouse", start: point(event.clientX, event.clientY, target), x, y};
                stop(event);
            }, options);
            target.addEventListener("mousemove", event => {
                if (gesture?.type !== "mouse") return;
                if (!event.buttons) { finish(event); return; }
                const current = point(event.clientX, event.clientY, target);
                x = gesture.x + current.x - gesture.start.x;
                y = gesture.y + current.y - gesture.start.y;
                stop(event);
                render();
            }, options);
            target.addEventListener("mouseup", event => {
                if (gesture?.type === "mouse") finish(event);
            }, options);
            target.addEventListener("click", event => {
                if (Date.now() < blockedUntil) stop(event);
            }, options);

            target.addEventListener("wheel", event => {
                if (event.ctrlKey) {
                    zoom(scale * Math.exp(-event.deltaY * 0.01), point(event.clientX, event.clientY, target));
                    stop(event);
                } else if (Math.abs(scale - 1) > 0.001) {
                    x -= event.deltaX; y -= event.deltaY;
                    stop(event);
                    render();
                }
            }, options);

            // WebKit's legacy pinch events also occur on iPads with trackpads.
            target.addEventListener("gesturestart", event => {
                if (!gesture) gesture = {type: "legacy", scale};
                stop(event);
            }, options);
            target.addEventListener("gesturechange", event => {
                if (gesture?.type === "legacy") zoom(gesture.scale * Number(event.scale || 1));
                stop(event);
            }, options);
            target.addEventListener("gestureend", event => {
                if (gesture?.type === "legacy") finish(event);
            }, options);
        }

        install(viewport);
        const observer = new ResizeObserver(() => render());
        observer.observe(viewport);
        render();
        return {
            install,
            zoomBy: delta => zoom(scale + delta),
            fit: () => { scale = 1; x = 0; y = 0; gesture = null; render(); },
            blocksNavigation: () => Boolean(gesture) || Math.abs(scale - 1) > 0.001 || Date.now() < blockedUntil,
            getScale: () => scale
        };
    };
})();
