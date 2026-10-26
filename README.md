# District 4 Tool

A Flask application served by Gunicorn, with Google Sheets mirrors, Google Drive
libraries, Gemini assistance, and SQLite storage. No separate database server is
needed. Run one container: background jobs and their progress use process-local
state, so the image deliberately runs one Gunicorn worker with four threads.

## Build and publish from your development machine

Run these commands inside `district4-tool`. Keep your existing `.env` and add
`DOCKER_IMAGE` and `IMAGE_TAG`, or create `.env` from `.env.example` on a fresh
checkout. Set `DOCKER_IMAGE` to the Docker Hub repository you own; its default is
`pijeme/district4-tool-main`. Choose a version tag for each release.

```dotenv
DOCKER_IMAGE=pijeme/district4-tool-main
IMAGE_TAG=1.0.0
```

```sh
docker login
docker build -t pijeme/district4-tool-main:1.0.0 .
docker push pijeme/district4-tool-main:1.0.0
```

Use your own repository and tag in both commands, matching `DOCKER_IMAGE` and
`IMAGE_TAG` in `.env`. Build directly with the Dockerfile on your development
machine. The project's single `compose.yml` pulls the published image, so the
server only needs `compose.yml`, `.env` and `service_account.json`.

The Dockerfile installs the pinned dependencies in `requirements.lock.txt`. It
copies application code, templates and static assets only. Credentials, local
databases, reports and caches are excluded from the image. The application runs
as UID/GID 10001, and logs go to Docker with size limits.

For a Linux server with a different CPU architecture, build explicitly. To
publish one image supporting both amd64 and arm64:

```sh
docker buildx create --name district4-builder --driver docker-container --use
docker buildx build --platform linux/amd64,linux/arm64 --tag YOUR_USERNAME/district4-tool:1.0.0 --push .
```

## Deploy with three files

Your server needs Docker Engine and the Docker Compose plugin. Create a directory
and put only these files inside it:

```text
district4/
  compose.yml
  .env
  service_account.json
```

Copy the repository's `compose.yml`. Create `.env` using `.env.example` as a
reference. Fill in:

- `DOCKER_IMAGE`: your Docker Hub repository, without a tag or URL prefix.
- `IMAGE_TAG`: the tag you published; defaults to `latest`.
- `FLASK_SECRET_KEY`: a long random secret; keep it stable across updates.
- `GEMINI_API_KEY`: required by the application at startup.
- `PLEDGE_SCRIPT_URL` and `PLEDGE_API_TOKEN`: if you use Thanksgiving Pledges.
- `TEMP_EDIT_USER_TOKEN` and `TEMP_EDIT_ADMIN_TOKEN`: set private values if you use
  temporary editing. Blank values disable access to the corresponding routes.

Generate a Flask secret on your development machine:

```sh
python -c "import secrets; print(secrets.token_hex(32))"
```

Copy your existing `service_account.json` to the server directory, or paste its
complete JSON contents into a file with that name. Compose mounts the file
read-only at `/app/service_account.json` and configures the application to use
it. Google Sheets and Drive features require valid service account credentials.
Share the spreadsheet and library folders with that service account's email.

If you previously set `GOOGLE_SERVICE_ACCOUNT_BASE64` or
`GOOGLE_SERVICE_ACCOUNT_JSON` in `.env`, remove those entries to use the mounted
file. The file must exist before starting Compose; a missing file causes an
error instead of creating a directory. Keep it readable by the container's
UID 10001. The credential file is excluded from Git and the Docker image.

For a private Docker Hub repository, run `docker login` on the server once.
Then, from the server project directory:

```sh
docker compose pull
docker compose up -d
docker compose ps
```

The default URL is `http://SERVER_IP:5000`. Change `APP_PORT` in `.env` to use a
different host port. If an existing local reverse proxy serves the application,
set `APP_BIND_ADDRESS=127.0.0.1`. The health endpoint is `/healthz`; it checks
the web process without calling Google services. Inspect logs with
`docker compose logs --tail=100 -f district4`.

No checkout, Dockerfile, Python installation, or database files are needed on
the server for a fresh deployment. The first application
request creates the SQLite schema and bootstraps the Sheets mirrors. With a
fresh volume, local-only records and Drive library indexes start empty; use the
app's sync tools to populate libraries. Existing local-only data requires the
one-time migration below.

## Persistent data and updates

Compose creates a named volume called `<project-name>_district4-data` at `/data`.
It contains `app_v2.db`, `ai_index.db`, `pastor_resource_sync_state.db`, SQLite
journal/lock files, `generated_reports/`, and `book_thumbnail_cache/`. New images
reuse this volume. Docker documents this [volume lifecycle](https://docs.docker.com/engine/storage/volumes/).

For each update, publish the new image, change `IMAGE_TAG` if necessary, then:

```sh
docker compose pull
docker compose up -d
```

Keep the server directory/project name stable so Compose selects the same data
volume. `docker compose down` retains it; `docker compose down -v` deletes it.

## One-time migration from the old deployment

Stop the old application before collecting its data so SQLite journals are not
changing. Keep a backup of the old files. Copy its databases and matching
`-wal`/`-shm` files, if present, plus reports and thumbnail cache into a temporary
`migration/` directory on the server. Do not put credentials in that directory.

For a new, empty deployment volume:

```sh
docker compose pull
docker compose create district4
docker compose cp ./migration/. district4:/data/
docker compose run --rm --no-deps --user 0 district4 chown -R 10001:10001 /data
docker compose up -d
```

Copying data is needed once when retaining existing records. It is not part of
normal deployments. Do not import into an active or already populated volume.

## Activity, notifications and Pij

The signed-in AO account `pijeme` has an **Activity & Updates** entry in AO Tools.
Accounts expand into paginated timelines. Successful page visits, ebook download
requests, and device share actions are recorded with account names and timestamps.
These activity records expire after 90 days. Successful website changes also
produce notifications, retained until cleared. One **Clear Activity & Notifications**
control removes both histories without deleting application records. Tracking
starts with this release; previous activity is not reconstructed. The owner popup
counts unread change notifications only and links directly to the notification tab.
Cancelled native sharing is not recorded as a completed share; download records
do not establish that a browser saved a file or a recipient opened it.

Pij uses read-only library retrieval, two concurrent AI requests, and one active
request per account. Model attempts have 20-second network timeouts with a shared
45-second budget and a single fallback model. Chat close clears its conversation;
late responses are discarded, and inactive history expires after 30 minutes.
Book recommendations include up to 10 relevant titles with source links; explicit
requests for fewer are respected. Pij's About Developer knowledge uses the same
public profiles and support details displayed by the page.
Pij also shares the pastor/church directory across all areas with every signed-in
role: pastor and church names, church addresses, contact numbers, birthdays, area
numbers and sub-areas. Report/account access keeps its existing role scope; the
shared directory uses an explicit field allowlist and excludes other account data.
Library answers match the requested subject, audience and resource type first.
Children's ministry and resource-type lists (such as "10 study Bibles") browse
verified available catalogue entries without requiring a matching text passage;
availability does not imply that the book's contents are searchable. Topic
questions still use indexed evidence. Commentaries/devotionals get preference
among similarly suitable sources, and extra matching pages add no ranking points.
Before replies are saved or returned, reader links are checked against the exact
titles and URLs supplied for that request. Unambiguous title matches can repair
wrong IDs; unverifiable links or exposed retrieval reports produce a concise
verified list without another AI call.
Recommendations include a short explanation per book. Valid model explanations
are retained when links need repair; bare items and validation fallbacks use
conservative catalogue-use notes or a short retrieved excerpt. Explicit requests
for titles only keep the list compact. Brackets in book titles render as links.
Broad topic recommendations do not use fuzzy single-title selection, and matching
uses whole words. Repeated imported copies of a title appear once. Study-Bible
lists include bounded opening excerpts when indexed, so descriptions can explain
edition differences; a bad list entry no longer removes valid explanations from
the other entries.
Runtime `pij_request` / `pij_provider` logs contain stage durations and error
categories without prompts or credentials. The existing single-worker Gunicorn
configuration remains required for the process-local request reservations.

## Backup and restore

Bible Study supports the reader-only Cebuano `CEBOCB` (Ang Pulong sa Dios) and
Tagalog `TCB` (Ang Salita ng Diyos) editions alongside NIV. The selector beside
passage navigation preserves the passage and saves the edition per account.
Verse popups and copying follow that selection. Original English study resources
continue to follow book/chapter; combined verses share the corresponding Thompson
chain topics. Native-language concordance uses case-insensitive exact word
occurrences. Native Word Study opens the selected translation's own dictionary
using recorded headwords/forms, without applying English stemming or English
part-of-speech rules. Readers choose an English equivalent from a definition to
search the Bible dictionaries; no English meaning is automatically assigned to
the verse. Missing forms offer explicitly unconfirmed spelling suggestions.
A persistent notice explains that the English meaning may not fit the verse
and may be less precise than an English Bible lookup. Saves retain their
original edition. USFM footnotes, headings, line breaks and verse ranges survive
import, and publisher attribution appears below each chapter.

Keep the supplied USFM ZIPs in the separate translation Drive folder
`1ew6gje-IGqNJ9J5wR2NGYV0_dUVJ5vIM`; they need not be extracted in Drive. Developer
**Sync Bible Study** imports them alongside the existing independent study sources.
For a translation-only import, run inside the deployed environment:

```sh
python bible_study.py sync-translations --database /data/bible_study_data/bible_study.db
```

The importer validates all 66 books and 1,189 chapters in a temporary database,
backs up the private Bible database before upgrading/publishing, and retains old
editions and personal records. Identical archives reuse the current edition and
do not create another backup. Startup and reader requests do not migrate schemas.
Deploy the updated application and run a sync on the server; local imports do not
modify the server's copy.

Developer **Sync details** shows the server job's current file/stage, prepared
source counts, and measured download bytes, EPUB sections, dictionary records or
database pages. Percentages describe the current stage, not estimated time or
whole-job completion; stages without a total use an indeterminate bar. Closing
and reopening the dialog or page follows the same job without starting another
sync. Progress snapshots live beside the private database as
`bible_study.sync-progress.json`, so publication transactions do not block progress
reads. Keep this file in the persistent data volume and outside Git/images.
A stopped worker is reported as interrupted; reopening does not restart it.
All signed-in users see a shared Bible Study update notice while the sync lock is
held. The notice endpoint exposes only the active flag and a public message;
detailed progress remains developer-only. It clears when the job ends.

Keep HarperCollins, Mounce, Vine, the BE series EPUBs, and
`kaikki.org-dictionary-Tagalog.jsonl` / `kaikki.org-dictionary-Cebuano.jsonl`
directly inside **For Scripture Study App**. Bible Study imports only these
supported resources, preserves article images/tables and BE typography, and
serves BE commentary from its own database.

BE article typography is selected using complete source paragraphs in article
order. Short headings mentioned inside prose cannot pull neighboring headings
into the reader. This correction also applies read-only to existing imports;
no resync is needed to remove those stray headings.

NIV Word Study shows Tagalog and
Cebuano possible equivalents from reverse English-definition searches, followed
by HarperCollins, Vine, and Mounce. Each original article has a compact preview;
**Read more** expands the complete article inline in source order. Articles are
not divided into separate definitions. The Kaikki entries retain Wiktionary/Kaikki attribution.
Word Study identifies HarperCollins with sky blue, Vine with mint green, and
Mounce with pink backgrounds; dark mode uses darker versions of those colors.
HarperCollins keeps paragraphs, source emphasis, images and tables in a responsive
layout rather than reproducing fixed EPUB pages. Printed **See / See also**
headwords link to articles from the same dictionary edition in a popup with Back;
the underlying Word Study stays in place. Bible citations open verse previews;
chapter-only citations open the chapter. Shortened chapter/verse citations inherit
their book only when supported by a preceding explicit citation. Navigation is
added at read time, so existing imports need no resync.
Mounce and Vine additionally preserve EPUB paragraph indentation, testament and
lettered grammatical headings, source emphasis, and the relative sizes of inline
Hebrew/Greek image glyphs. Expanded articles include all their original sections
and word types, without regrouping them. Distinct articles in the original EPUB
remain separate. Internal EPUB cross-references resolve within the same dictionary edition.
Sync Bible Study refreshes formatting for already indexed editions after checking
that every article boundary, source text and asset is unchanged. It retains entry
and sense IDs, validates a private SQLite copy, and makes a recovery backup before
updating the live Bible Study database.

Tapping a component of a source-named phrase (such as "lapis lazuli") searches
the full phrase and highlights its component words without changing verse text.
Word Study separates nouns, names, verbs, adjectives, adverbs and grammar words.
Auto uses printed dictionary labels and conservative English verse-context rules;
ambiguous words keep all word types. A Word type selector lets the reader override
the suggestion for translation equivalents and background relevance. Mounce and
Vine always show complete articles, regardless of the selected word type.
Native-language equivalents follow the selected type. Person profiles appear for names.
Cross-references are not direct English headword matches. When no dictionary
heading matches, article-text results appear in a collapsed **Related articles
mentioning this word** section with the actual article titles. Grammatical words
show their English function and omit article-text searches. These suggestions do
not establish an NIV-to-original-language alignment.
Thompson concordance also finds printed headings inside
numerical chain topics, with links to open the original topic. These lookup
improvements work with already imported sources and do not require another sync.

The study folder and its descendants are excluded from the AI scan and cached
AI retrieval. BE EPUBs are also excluded independently of their old catalog
location; BE PDFs outside the study folder remain eligible for AI indexing.
Website Bible Study sync leaves the AI database unchanged. The explicit maintenance
CLI can still remove successfully migrated editions' AI document, chunk, search,
and formatting records after validation. To request that cleanup, include its path:

```sh
python bible_study.py sync-drive --database /data/bible_study_data/bible_study.db --ai-database /data/ai_index.db
```

**Sermon Illustrations** appears after Sermon Outlines. Keep
`sermon_illustrations_codex_v2.zip` intact, directly inside **For Scripture Study
App**. Sync Bible Study recognizes this v2 package and skips older illustration
archives. It validates a staged copy, recreates approved tables and the FTS index,
retains all 8,961 original IDs/articles and their topic mappings, and publishes a
separate private `sermon_illustrations.db` alongside `bible_study.db`. Existing
illustration libraries receive recovery backups before replacement. Repeating an
unchanged import does not duplicate records. Reading requests never create storage.
An optional absolute `BIBLE_ILLUSTRATIONS_DATABASE` overrides the default location;
it must differ from the Bible Study, AI, application, and library database paths.

The full Sermon Outlines and Sermon Illustrations tab names wrap onto two lines
on mobile. The illustration tab has two choices: **Suggested Illustrations** follows the selected chapter,
verse, or verse range, showing ten results and **See More** up to thirty total;
**Browse by Topic** offers A–Z, topic/title/body search, collection filtering, and
paged article lists. All records remain reachable through topic browsing, including
the empty source record, which is labeled for review. Passage matching respects
cross-chapter endpoints. Identical normalized bodies are grouped in suggestions;
their individual source records remain discoverable through browsing.

The mobile study tabs share five equal columns, with both Sermon labels on two
lines so every tab fits without horizontal scrolling. The initial ten suggestions
show up to five from 1001 Illustrations That Connect first, then the five strongest
matches pooled from the other two collections. The next twenty use the strongest
remaining matches across all three collections, without source quotas. Missing
matches are filled from available relevant entries, without adding unrelated cards.
Source-provided references stay separate from topical connections. The owner approved
all pending links for pastors to use and assess; this approval is retained on future
imports and does not change source provenance or confidence into a verified claim.
Approved low-confidence links are also eligible, with stronger matches ranked first.
Approval is recorded in illustration metadata and the live update receives a recovery
backup. Invalid links, including
the source's `Isaiah 0:12–14`, are retained for review and excluded from navigation
and passage matching. Original article wording and reference metadata are unchanged.
Full articles use the safe block renderer to preserve paragraphs/emphasis without
executing imported HTML. Copy includes the original text and source credit. Search
uses local SQLite and does not require an AI call or change the AI index.

For an illustration-only local import (the target is its own database):

```sh
python bible_study.py import-illustrations --database /data/bible_study_data/sermon_illustrations.db --archive /path/sermon_illustrations_codex_v2.zip
```

Add `--dry-run` to validate without publishing. Local imports do not update a
deployed server; deploy the code and use Sync Bible Study there for the Drive ZIP.

The translation folder and supplied file IDs are excluded before recursive
library scanning. AI indexing and retrieval also exclude restricted IDs and
matching source metadata, including stale cached documents. These translations
are stored only in the private Bible Study database, never copied into the AI
index or sent to an AI provider. Pij's NIV-only passage helper remains NIV-only.

For a consistent full backup, stop the application briefly and copy the volume:

```sh
docker compose stop district4
docker compose cp district4:/data ./district4-backup
docker compose start district4
```

To restore a backup into an empty volume, use the migration commands above with
`./district4-backup/.` as the source. Back up before updates that change schemas;
an older image may not support a database upgraded by a newer release.
