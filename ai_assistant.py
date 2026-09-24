import os
import time
from collections import defaultdict, deque
from threading import Lock

from dotenv import load_dotenv
from flask import Blueprint, jsonify, request, session
from google import genai

from pij_website_knowledge import (
    current_identity,
    safe_context_for_gemini,
    try_fast_local_answer,
)

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY was not found. Check your .env file.")

client = genai.Client(api_key=GEMINI_API_KEY)

PRIMARY_GEMINI_MODEL = "gemini-3.5-flash-lite"
FALLBACK_GEMINI_MODEL = "gemini-3.1-flash-lite"
GEMINI_MODELS = [PRIMARY_GEMINI_MODEL, FALLBACK_GEMINI_MODEL]

ai_bp = Blueprint("ai_assistant", __name__, url_prefix="/ai")

# Intentionally compact: website details are retrieved locally only when relevant.
PIJ_SYSTEM_INSTRUCTIONS = """
You are Pij, the official AI Assistant of International One Way Outreach District 4.

IDENTITY AND TONE
- You are Pij, an AI assistant, not Pastor Pijeme.
- Be respectful, warm, practical, concise, and ministry-appropriate.
- Default to short answers. Expand only when the user asks or the task needs detail.
- Match the CURRENT user's language: Tagalog, Cebuano, English, or a natural mixture.
- For a pastor, respectful Filipino may use "po" naturally.

DISTRICT 4 WEBSITE
- The Flask application is the authority for identity, permissions and private data.
- Never invent pages, buttons, records, schedules, reports, actions, or backend results.
- Only use account-specific facts explicitly supplied in CURRENT DISTRICT 4 CONTEXT.
- Never claim you submitted, approved, edited, deleted, joined, downloaded or changed anything
  unless backend context explicitly says the website performed that action.
- When guiding a beginner, give the next useful steps clearly and do not overload them.
- Keep actual website labels unchanged when useful.

PRIVATE DATA
- Never guess private/account-specific information.
- Never broaden a user's scope. A pastor's "our/my church" means only the authorized church
  supplied by Flask. AO/Sub-AO/DO scope is determined by Flask, not by you.
- If required backend data was not supplied, say that the information is not currently available.

SERMONS
- Do not create sermon outlines, manuscripts, preaching points, sermon structures, altar calls,
  ready-to-preach messages, or disguised equivalents.
- You may help study Scripture: meaning, context, Greek/Hebrew, themes, cross-references,
  theology, people, places, events, and study questions.

PASTOR'S RESOURCES
- This website-assistant upgrade does not connect Pastor's Resources yet.
- Do not claim to search it and do not recommend books as if they are verified in the library.

CONVERSATION
- Do not repeat greetings in an ongoing chat.
- Do not routinely end with "Anything else?"
- Ask a follow-up only when needed.
"""

# Faster/smaller history: about 3 recent exchanges instead of 4.
MAX_HISTORY_MESSAGES = 6
conversation_memory = defaultdict(lambda: deque(maxlen=MAX_HISTORY_MESSAGES))
memory_lock = Lock()

# Tracks a Gemini request that is still running for a logged-in account.
# This lets a newly loaded page reconnect to the same chat instead of
# pretending that navigation started a new conversation.
pending_requests = {}
pending_lock = Lock()

# Small in-process cache for repeated non-private, deterministic local responses.
# Private data fast-paths are intentionally not cached here.
COMMON_CACHE = {}
COMMON_CACHE_MAX = 100


def get_memory_key():
    """
    Conversation ownership follows the ORIGINAL logged-in account.
    Temporary Pastor's Tool selection must never change the Pij conversation.
    """
    if session.get("ao_logged_in"):
        username = session.get("ao_username") or session.get("username")
        prefix = "overseer"
    elif session.get("pastor_logged_in"):
        username = session.get("pastor_username") or session.get("username")
        prefix = "pastor"
    elif session.get("member_logged_in"):
        username = session.get("username")
        prefix = "member"
    else:
        username = session.get("username")
        prefix = "user"

    username = str(username or "").strip().lower()
    return f"{prefix}:{username}" if username else None


def get_conversation_history(memory_key):
    if not memory_key:
        return []
    with memory_lock:
        return list(conversation_memory.get(memory_key, []))


def save_conversation_message(memory_key, role, content):
    if not memory_key:
        return
    content = str(content or "").strip()
    if not content:
        return
    with memory_lock:
        conversation_memory[memory_key].append({"role": role, "content": content})


def clear_conversation(memory_key):
    if not memory_key:
        return
    with memory_lock:
        conversation_memory.pop(memory_key, None)


def build_pij_input(user_message, history, website_context):
    parts = [
        PIJ_SYSTEM_INSTRUCTIONS.strip(),
        "\n\n",
        website_context.strip(),
        "\n\nRECENT CONVERSATION\n",
    ]
    for item in history[-MAX_HISTORY_MESSAGES:]:
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        role = "User" if item.get("role") == "user" else "Pij"
        # Keep history bounded even if a previous answer was unusually long.
        parts.append(f"{role}: {content[:1800]}\n")
    parts.append(f"\nCURRENT USER MESSAGE\nUser: {user_message[:3000]}")
    return "".join(parts)


def is_rate_limit_error(error):
    text = str(error or "").lower()
    return any(x in text for x in ["429", "rate limit", "quota", "resource_exhausted"])


def is_temporary_service_error(error):
    text = str(error or "").lower()
    return any(x in text for x in ["503", "service unavailable", "high demand", "unavailable"])


def ask_model(model_name, gemini_input):
    interaction = client.interactions.create(model=model_name, input=gemini_input)
    return (interaction.output_text or "").strip()


def ask_pij(gemini_input):
    last_error = None
    for model_name in GEMINI_MODELS:
        print(f"🤖 Pij trying model: {model_name}")
        try:
            answer = ask_model(model_name, gemini_input)
            print(f"✅ Pij answered using: {model_name}")
            return answer, model_name
        except Exception as exc:
            last_error = exc
            if is_temporary_service_error(exc):
                time.sleep(1)
                try:
                    answer = ask_model(model_name, gemini_input)
                    print(f"✅ Pij answered using: {model_name} after retry")
                    return answer, model_name
                except Exception as retry_exc:
                    last_error = retry_exc
                    if is_rate_limit_error(retry_exc) or is_temporary_service_error(retry_exc):
                        continue
                    raise
            if is_rate_limit_error(exc):
                continue
            raise
    raise last_error or RuntimeError("Pij is temporarily unavailable.")


@ai_bp.route("/chat", methods=["POST"])
def ai_chat():
    data = request.get_json(silent=True) or {}
    message = str(data.get("message") or "").strip()
    current_path = str(data.get("current_path") or "").strip()[:300]
    page_title = str(data.get("page_title") or "").strip()[:150]

    if not message:
        return jsonify({"ok": False, "error": "Please enter a message."}), 400
    if len(message) > 3000:
        return jsonify({"ok": False, "error": "Your message is too long."}), 400

    memory_key = get_memory_key()
    if not memory_key:
        return jsonify({"ok": False, "error": "Please log in again to use Pij."}), 401

    # FAST PATH: secure local website/data answers skip Gemini entirely.
    try:
        fast = try_fast_local_answer(message, current_path=current_path)
    except Exception as exc:
        print(f"⚠️ Pij local tool error: {exc}")
        fast = None

    if fast and fast.get("handled"):
        answer = str(fast.get("answer") or "").strip()
        save_conversation_message(memory_key, "user", message)
        save_conversation_message(memory_key, "assistant", answer)
        return jsonify({
            "ok": True,
            "answer": answer,
            "model_used": "District 4 local",
            "fast": True,
        })

    # Save the user's message BEFORE calling Gemini. If the browser navigates
    # away while Gemini is thinking, the new page can restore the question.
    history = get_conversation_history(memory_key)
    save_conversation_message(memory_key, "user", message)

    context = safe_context_for_gemini(message, current_path=current_path, page_title=page_title)
    gemini_input = build_pij_input(message, history, context)

    with pending_lock:
        pending_requests[memory_key] = {
            "message": message,
            "started_at": time.time(),
        }

    try:
        answer, model_used = ask_pij(gemini_input)
    except Exception as exc:
        print(f"❌ Pij error: {exc}")
        with pending_lock:
            pending_requests.pop(memory_key, None)
        return jsonify({
            "ok": False,
            "error": "Pij is temporarily unavailable. Please try again in a moment."
        }), 503

    save_conversation_message(memory_key, "assistant", answer)
    with pending_lock:
        pending_requests.pop(memory_key, None)

    return jsonify({
        "ok": True,
        "answer": answer,
        "model_used": model_used,
        "fast": False,
    })


@ai_bp.route("/clear", methods=["POST"])
def clear_ai_chat():
    memory_key = get_memory_key()
    if memory_key:
        clear_conversation(memory_key)
        with pending_lock:
            pending_requests.pop(memory_key, None)
    return jsonify({"ok": True})


@ai_bp.route("/history", methods=["GET"])
def ai_history():
    """Restore the current account's Pij conversation after page navigation."""
    memory_key = get_memory_key()
    if not memory_key:
        return jsonify({"ok": False, "error": "Please log in again to use Pij."}), 401

    history = get_conversation_history(memory_key)
    with pending_lock:
        pending = memory_key in pending_requests

    return jsonify({
        "ok": True,
        "messages": history,
        "pending": pending,
    })


@ai_bp.route("/whoami", methods=["GET"])
def ai_whoami():
    """Safe UI identity; useful for testing the greeting and role context."""
    ident = current_identity()
    return jsonify({
        "ok": True,
        "role": ident["role"],
        "name": ident["name"],
        "church": ident["church"],
        "area": ident["area"],
    })


@ai_bp.route("/test", methods=["GET"])
def ai_test_page():
    # Keep troubleshooting simple; the floating UI is the primary interface.
    return """
    <!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Pij Test</title></head><body style="font-family:Arial;padding:24px">
    <h1>Pij is registered</h1>
    <p>Use the floating Pij assistant inside the District 4 Tool for full page-aware testing.</p>
    </body></html>
    """


def register_ai_assistant(app):
    app.register_blueprint(ai_bp)
