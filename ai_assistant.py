import os
import re
import time
import json
from collections import defaultdict, deque
from threading import Lock
from dotenv import load_dotenv
from flask import Blueprint, jsonify, request, session
from google import genai
from pij_website_knowledge import (

    current_identity,

    safe_context_for_gemini,

)

from pij_library_knowledge import (

    library_context_for_gemini,

    register_pij_library_routes,

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



# Gemini is deliberately the conversational brain in this experiment.

PIJ_SYSTEM_INSTRUCTIONS = """

You are Pij, the official AI Assistant of International One Way Outreach District 4.



CORE ROLE

- You are Pij, an AI assistant, not Pastor Pijeme.

- Be respectful, warm, practical, concise, and ministry-appropriate.

- Think and reason freely from the information Flask gives you. Do not behave like a keyword echo bot.

- Synthesize, compare, explain, and draw reasonable conclusions from supplied evidence when that helps the user.

- General Bible/theology knowledge may be used for ordinary study questions. If the user specifically asks what the District 4 database, website, or Pastor's Resources says, ground that part of the answer in the supplied context.



LANGUAGE

- A CURRENT RESPONSE LANGUAGE instruction is supplied immediately before every current user message.

- Follow that instruction for THIS response even if earlier conversation messages used another language.

- Keep proper names, official website labels, and book titles unchanged when appropriate.

- A respectful Filipino tone may use "po" naturally only when it fits the selected language.



DISTRICT 4 WEBSITE

- CURRENT DISTRICT 4 CONTEXT contains the website operating manual plus a sanitized, role-authorized database snapshot.

- Treat the website manual as authoritative knowledge of how the District 4 Tool works.

- You may freely explain and combine known website steps into a useful workflow.

- Do not invent a page, button, field, action, status, route, or successful backend result that is not supported by the supplied website context.

- Flask remains the authority for identity and data permissions. Never broaden the logged-in user's data scope.

- You can guide a user through actions, but do not claim that you personally submitted, approved, edited, deleted, joined, downloaded, or changed data unless backend context explicitly confirms an action occurred.



PRIVATE / ACCOUNT DATA

- Use private/account-specific facts only when Flask supplied them in the authorized snapshot.

- A pastor's "our/my church" means the pastor's authorized church. AO/Sub-AO/DO scope is determined by Flask.

- If a required private record is absent, say that the record is not available in the current authorized data rather than guessing it.



SERMONS AND STUDY

- Do not CREATE a new ready-to-preach sermon outline, sermon manuscript, preaching-point structure, altar call, or disguised equivalent.

- This restriction does NOT prevent retrieval. You MAY find, cite, summarize, compare, and explain EXISTING sermons, sermon outlines, sermon illustrations, preaching material, or commentaries that are actually retrieved from Pastor's Resources.

- Public sermon ebooks in Pastor's Resources are normal library sources for authorized users.

- A separate private created-sermon collection may appear only when Flask authorizes the logged-in account. Never mention or expose that private collection unless its material is explicitly supplied in CURRENT PASTOR'S RESOURCES CONTEXT.

- You may freely help with Scripture study: context, meaning, Greek/Hebrew, theology, themes, cross-references, people, places, events, study questions, and comparison of retrieved resources.



PASTOR'S RESOURCES

- Pastor's Resources is connected through Flask-controlled retrieval.

- For broad topic/recommendation questions, prefer several distinct relevant books when several are supplied; do not collapse a broad request into one accidental source.

- For synthesis questions, compare recurring ideas across the supplied books and explain the synthesis in your own words.

- For a question naming one specific book, stay focused on that exact catalog book unless the user asks for comparison. Never substitute another book.

- Claims about what a particular library book contains must be supported by retrieved evidence from that book.

- You may use your own reasoning to explain retrieved material, but never invent a book, author, page, quotation, or claim of library availability.

- When an APPROVED LINK is supplied, make the relevant book/source clickable using Markdown: [label](/exact-approved-path).

- For PDF evidence, use the supplied exact-page link when available. For EPUB evidence, use the supplied section link.

- Never invent, alter, shorten, or guess a Pastor's Resources URL.

- If CURRENT PASTOR'S RESOURCES CONTEXT says WEB_FALLBACK_ALLOWED: YES, the named book is confirmed in the catalog but its contents are not searchable locally. You may use Google Search for information about that EXACT book.

- When using that fallback, clearly tell the user that the information is from external sources, may differ from the edition in Pastor's Resources, and should be verified against the actual ebook.

- Never present web information as an exact quotation, page number, or passage from the library copy unless local indexed evidence supplied it.

- Pastor's Resources evidence is retrieved locally before this response so most user messages require only one Gemini request.

- Flask performs the local Pastor's Resources lookup before this response and supplies the resulting exact-book or topic evidence in CURRENT PASTOR'S RESOURCES CONTEXT.

- For a named book, trust the exact catalog identity and indexed passages supplied by Flask. Never substitute a different book because another source appears more relevant.

- If Flask says the named book exists but is not searchable locally and WEB_FALLBACK_ALLOWED: YES is present, you MAY use Google Search for general information about that exact book. Clearly label that information as external and tell the user to verify it against the actual library ebook.

- Never present web information as an exact quotation, page number, or exact content of the library copy.

- If a named book is NOT found in the Pastor's Resources catalog, do not call it a library book and do not recommend/promote it as though it were available.

- For scanned/image-only books, catalog availability and indexed-text availability are different facts. State the difference clearly.



CLICKABLE WEBSITE LINKS

- CURRENT DISTRICT 4 CONTEXT may provide APPROVED INTERNAL WEBSITE LINKS.

- When guiding a user to a named page or feature, make the label clickable when its approved path is supplied.

- Use Markdown links only for exact relative paths supplied by Flask.

- If no approved link is supplied, write the label as ordinary text.

- A clickable link never bypasses authorization; Flask still decides whether the logged-in account may open it.



FORMATTING

- If you use an ordered list, number top-level items consecutively (1, 2, 3...).

- For book recommendations, normally give 3 or more distinct books when the user asks broadly and enough relevant sources were retrieved.

- Keep answers readable rather than dumping raw database rows or long ebook passages.



CONVERSATION

- Do not repeat greetings in an ongoing chat.

- Do not routinely end with "Anything else?"

- Ask a follow-up only when genuinely needed.

"""



# Faster/smaller history: about 3 recent exchanges instead of 4.

MAX_HISTORY_MESSAGES = 6

conversation_memory = defaultdict(lambda: deque(maxlen=MAX_HISTORY_MESSAGES))

memory_lock = Lock()



# A request can continue on the server after the browser navigates to another page.

pending_requests = {}

pending_lock = Lock()



# Small in-process cache for repeated non-private, deterministic local responses.

# Private data fast-paths are intentionally not cached here.

COMMON_CACHE = {}

COMMON_CACHE_MAX = 100





def get_memory_key():

    """Conversation ownership follows the original authenticated login."""

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





def detect_response_language(message):

    """Choose the response language from the CURRENT message only.



    This is intentionally lightweight and deterministic. Gemini still handles

    natural phrasing; Flask simply prevents old conversation language from

    pulling a new English/Cebuano/Tagalog question into the wrong language.

    """

    text = str(message or "").strip().lower()

    words = re.findall(r"[a-zà-ÿ']+", text)

    if not words:

        return "English"



    english = {

        "the", "a", "an", "and", "or", "is", "are", "was", "were", "be",

        "can", "could", "would", "should", "how", "what", "when", "where",

        "why", "who", "which", "do", "does", "did", "give", "show", "tell",

        "suggest", "recommend", "find", "book", "books", "about", "from",

        "with", "for", "this", "that", "my", "our", "your", "report",

        "approve", "approved", "attendance", "church", "area", "month",

    }

    tagalog = {

        "ako", "ko", "akin", "ikaw", "mo", "iyo", "kami", "namin", "atin",

        "ano", "paano", "saan", "kailan", "bakit", "sino", "alin", "pwede",

        "maaari", "bilang", "gusto", "bigay", "ibigay", "hanap", "hanapin",

        "tungkol", "mula", "para", "nasa", "isang", "maging", "alam", "ba",

        "po", "opo", "ito", "iyon", "mismo", "simbahan", "buwan",

    }

    cebuano = {

        "unsa", "unsaon", "asa", "kanus", "kanus-a", "ngano", "kinsa", "hain",

        "pila", "nako", "nimo", "imong", "iyang", "among", "atong", "inyong",

        "ug", "nga", "kini", "kana", "adto", "gikan", "mahitungod", "bahin",

        "palihug", "hatagi", "ihatag", "pangita", "pangitaa", "pwede", "ba",

        "simbahan", "bulan", "karong", "sunod", "niining", "didto", "diri",

    }



    en = sum(1 for w in words if w in english)

    tl = sum(1 for w in words if w in tagalog)

    ceb = sum(1 for w in words if w in cebuano)



    # Strong language-specific markers outweigh shared Filipino/Cebuano words.

    if any(w in {"unsa", "unsaon", "nako", "nimo", "imong", "ug", "nga", "gikan", "palihug", "hatagi", "pila", "bulan", "karong", "kanus"} for w in words):

        ceb += 3

    if any(w in {"paano", "maaari", "bilang", "tungkol", "mula", "ibigay", "hanapin"} for w in words):

        tl += 3

    if any(w in {"how", "what", "when", "where", "why", "suggest", "recommend", "according"} for w in words):

        en += 3



    scores = {"English": en, "Tagalog": tl, "Cebuano": ceb}

    ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)

    best_lang, best_score = ordered[0]

    second_score = ordered[1][1]



    if best_score == 0:

        return "English"

    if best_score >= second_score + 2:

        return best_lang

    if best_lang == "English" and en >= 2:

        return "English"

    if best_lang == "Cebuano" and ceb >= 2:

        return "Cebuano"

    if best_lang == "Tagalog" and tl >= 2:

        return "Tagalog"

    return "Mixed"





def response_language_instruction(user_message):

    language = detect_response_language(user_message)

    if language == "Mixed":

        return (

            "CURRENT RESPONSE LANGUAGE: MIXED / MATCH CURRENT USER MESSAGE\n"

            "Use a natural mixture matching the CURRENT message. Do not copy the language "

            "of older conversation turns merely because they came earlier."

        )

    return (

        f"CURRENT RESPONSE LANGUAGE: {language.upper()}\n"

        f"Answer this turn in {language}. Ignore the language of older conversation turns "

        "when choosing the response language."

    )





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

        parts.append(f"{role}: {content[:1800]}\n")



    parts.append("\n" + response_language_instruction(user_message) + "\n")

    parts.append(f"\nCURRENT USER MESSAGE\nUser: {user_message[:3000]}")

    return "".join(parts)





def is_rate_limit_error(error):

    text = str(error or "").lower()

    return any(x in text for x in ["429", "rate limit", "quota", "resource_exhausted"])





def is_temporary_service_error(error):

    text = str(error or "").lower()

    return any(x in text for x in ["503", "service unavailable", "high demand", "unavailable"])





def ask_model(model_name, gemini_input, allow_web_fallback=False):

    """Use one Gemini interaction for the answer.



    Google Search is exposed only when Flask has already confirmed that a named

    book exists in Pastor's Resources but its contents are not searchable in the

    local Pij index. This keeps ordinary questions and indexed-library questions

    to one normal Gemini request and avoids the multi-round function-call loop.

    """

    kwargs = {

        "model": model_name,

        "input": gemini_input,

    }

    if allow_web_fallback:

        kwargs["tools"] = [{"type": "google_search"}]



    interaction = client.interactions.create(**kwargs)

    return (interaction.output_text or "").strip()





def ask_pij(gemini_input, allow_web_fallback=False):

    last_error = None

    for model_name in GEMINI_MODELS:

        print(f"🤖 Pij trying model: {model_name}")

        try:

            answer = ask_model(model_name, gemini_input, allow_web_fallback=allow_web_fallback)

            print(f"✅ Pij answered using: {model_name}")

            return answer, model_name

        except Exception as exc:

            last_error = exc

            if is_temporary_service_error(exc):

                time.sleep(1)

                try:

                    answer = ask_model(model_name, gemini_input, allow_web_fallback=allow_web_fallback)

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



    # Every question goes to Gemini. Flask only supplies identity, permissions,

    # website knowledge and a sanitized/authorized database snapshot.

    history = get_conversation_history(memory_key)

    save_conversation_message(memory_key, "user", message)



    try:

        context = safe_context_for_gemini(

            message,

            current_path=current_path,

            page_title=page_title,

        )

    except Exception as exc:

        print(f"❌ Pij context error: {exc}")

        return jsonify({

            "ok": False,

            "error": "Pij could not prepare the District 4 data context."

        }), 500





    # Lightweight local retrieval happens BEFORE Gemini. It does not consume

    # Gemini quota. Gemini still interprets the user's request and writes the

    # final answer, but normally only one Gemini interaction is required.

    try:

        library_context = library_context_for_gemini(message, history=history)

    except Exception as exc:

        print(f"⚠️ Pij library retrieval warning: {exc}")

        library_context = (

            "PASTOR'S RESOURCES AI RETRIEVAL\n"

            "- Library retrieval is temporarily unavailable for this question."

        )



    # Only expose Google Search when the local catalog confirms that the named

    # book exists but its ebook text is not searchable/indexed.

    allow_web_fallback = "WEB_FALLBACK_ALLOWED: YES" in library_context



    combined_context = (

        context

        + "\n\nCURRENT PASTOR'S RESOURCES CONTEXT\n"

        + library_context

    )



    gemini_input = build_pij_input(message, history, combined_context)



    with pending_lock:

        pending_requests[memory_key] = {

            "message": message,

            "started_at": time.time(),

        }



    try:

        answer, model_used = ask_pij(gemini_input, allow_web_fallback=allow_web_fallback)

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

    """Restore the same Pij conversation after page navigation."""

    memory_key = get_memory_key()

    if not memory_key:

        return jsonify({"ok": False, "error": "Please log in again to use Pij."}), 401

    history = get_conversation_history(memory_key)

    with pending_lock:

        pending = memory_key in pending_requests

    return jsonify({"ok": True, "messages": history, "pending": pending})





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

    register_pij_library_routes(app)
