# Docker image login setup

The published image creates an empty account cache on a fresh data volume. It
needs Google Sheets credentials to populate that cache before anyone can log in.
The image intentionally excludes `.env`, `service_account.json`, and local
databases. Supply credentials when running the image.

## Required files

Keep these files in the deployment folder:

- `compose.yml`: use `compose.image.yaml` from this folder, renamed to `compose.yml`.
- `.env`: your deployment environment settings, including `FLASK_SECRET_KEY`.
- `service_account.json`: your Google service account credentials. This account
  must have access to the `District4 Data` spreadsheet and the library Drive files.

The Compose file mounts the credentials read-only at `/app/service_account.json`.
It refuses to start if the host file is missing, instead of creating a directory
where the credential file should be. Keep the credential file and `.env` private;
do not commit them or include them in the Docker image.

The image also supports `GOOGLE_SERVICE_ACCOUNT_BASE64` or
`GOOGLE_SERVICE_ACCOUNT_JSON` in the runtime environment. If intentionally using
that option instead, remove the credential file mount. Avoid setting both an
environment credential and a file credential; the image prefers the environment.

## Update an existing deployment

Add the credential mount from `compose.image.yaml` to your existing Compose file.
Keep the existing Compose project name and `district4-data` volume so your saved
data stays attached. Then, from the deployment folder:

```powershell
docker compose config --quiet
docker compose up -d --force-recreate district4
```

Visit the site to trigger the initial Google Sheets sync, then sign in. A fresh
sync can take longer than later visits. If it fails, inspect:

```powershell
docker compose logs --tail 100 district4
```

Missing credentials or denied spreadsheet access leave the account cache empty;
the login screen can then show an invalid-credentials message even for a valid
account. Do not delete the data volume to resolve this.

## Run alongside Python locally

Python's default server and this Docker setup both use port 5000. Either stop one
before starting the other, or set `APP_PORT=5001` in the Docker deployment `.env`
and recreate the container. Docker will then be at `http://127.0.0.1:5001`, while
Python remains at `http://127.0.0.1:5000`.

## Bible Study updates

Rebuild the image after updating dependencies in both `requirements.txt` and
`requirements.lock.txt`; Docker installs the locked dependencies, including
`pypdfium2` for Thompson PDF indexing. The Compose data volume persists the Bible
database at `/data/bible_study_data/bible_study.db` and the commentary index at
`/data/ai_index.db`. Keep that volume attached when updating the image. No source ebooks belong in the
image, public static folder, or Git repository.

The developer **Sync Bible Study** button reads **For Scripture Study App**
directly from Drive. It prefers a uniquely named NIV Thompson Chain-Reference
PDF over the older NIV EPUB and keeps Mounce and Vine as independent sources.
The inspected 2022 Thompson PDF contains NIV 2011; other PDF layouts fail
validation rather than replacing the active reader.

The sync validates a temporary SQLite copy and creates a recovery backup in
`bible_study_data/backups/` before migrating an existing database. Old editions,
verse IDs, token offsets, STEP data, dictionary source content and personal
records are retained. Keep enough disk space for the temporary copy, backup,
and new edition. Repeating an unchanged sync does not duplicate editions.

An operator can also run `python bible_study.py sync-drive --database /absolute/private/bible_study.db`
without starting the main website. Ordinary reading requests never migrate the
schema or open the source PDF/EPUBs.

The study panel has Word Study, Concordance and Commentary tabs.
NIV word clicks use the selected tab and keep the selected word
when switching tabs. Word Study shows separate, expandable dictionary definitions. Matching uses
English headwords/inflections and testament-specific definitions; explicit
Scripture references and STEP identifiers can narrow candidates. These are
dictionary explanations; the matching logic does not create verified NIV word alignments.
Actual verified alignment records, if supplied, remain a separate path.

Commentary offers Wiersbe's Be Series and Exalting Jesus selectors below its
banner. Both read the existing private `AI_DATABASE` and follow the containing
verse of the selected word. Changing series preserves the passage; chapter
navigation retains the selected series. Full original section text appears
immediately, without Read more, AI generation or rewriting.
Wiersbe uses its explicit chapter/passage headings and prefers EPUB extraction.
Exalting Jesus uses its passage headings before Main Idea, supports inline titles
and epigraphs, and reads both EPUB and PDF volumes. Duplicate formats prefer
EPUB. Long sections and PDF page continuations are retained, and incidental
citations in prose do not become article coverage. Source metadata changes
invalidate the in-memory caches. The AI index is read-only; no schema migration
or reindexing is needed for these commentary readers.
Unmatched passages display an availability message without inventing commentary.

Exalting Jesus's opening outline is displayed in an indented panel. Matching
body headings use larger bold Roman-numeral sections and lettered/numeric
subheadings. Presentation metadata preserves the original indexed wording and
Scripture-reference offsets. Existing printed markers are retained; unnumbered
main body headings receive sequential Roman numerals. Duplicated numbering or
different titles in the source outline are not rewritten into the source text.

The availability/status strips no longer take space above the reader.
Developer-only Sync Bible Study and Sync details controls sit beside the layout
buttons. Availability, sync status, counts and warnings are in the Sync details
dialog, including in fullscreen. Ordinary users see no developer controls.

The Thompson concordance is imported separately from the numerical chain index
into schema version 5. It preserves printed headwords, related forms, short
excerpts and source references. The developer sync imports it, including when
an unchanged Thompson PDF is already indexed without a concordance. Existing
reader editions, verse/token IDs and personal records are retained.

To add the concordance using a private local copy of the same indexed PDF, run
`python bible_study.py import-concordance --database /absolute/private/bible_study.db --pdf /absolute/private/thompson.pdf`.
Add `--dry-run` to validate without changing the live database. This command
checks the source fingerprint, validates a temporary database and saves a recovery
backup before applying the migration. Do not put the PDF in the public static folder.

Related chain names/numbers open their topic in the same popup, with Back
navigation preserving scroll position. Clicking outside a popup or pressing
Escape closes the topmost popup. Full verse previews retain Copy Verse.

Schema version 6 adds Thompson book introductions for all 66 books, character
studies, name classifications, biographies and private source-chart images.
Book Study appears beside Bible and follows the selected book, preserving the
reader's position when switching tabs. The exact condensed-outline entry for
each book appears inline, including combined entries where the source groups
books together. Major Chains are arranged as linked rows. References to imported
character studies and journey charts use the same popup; unavailable printed
targets are identified explicitly. Person profiles appear above dictionary
results in Word Study; ambiguous identities are offered as explicit choices.
Shared verse citations alone do not merge different family identities.

The same maintenance workflow imports this material without replacing the
existing Bible editions: `python bible_study.py import-studies --database /absolute/private/bible_study.db --pdf /absolute/private/thompson.pdf`.
`--dry-run` validates a temporary copy first. The command verifies the exact
source fingerprint and saves a recovery backup before migration. Developer
Drive sync also detects and imports missing study material for an unchanged
Thompson source. Reading requests use SQLite, including authenticated chart
images; the PDF is never required by the reader at runtime.

Sync also refreshes unchanged sources whose book studies lack inline outlines.
Desktop Bible Study uses the available window width and height. On mobile,
horizontal swipes in the Bible text navigate chapters, including book boundaries;
vertical scrolling, text selection and pinch zoom remain native browser actions.

The bulletin's Daily Devotional uses a separate white panel. It shows an exact
opening excerpt from the matching Maxwell Leadership Bible article; Read more
shows the original indexed article. There is no AI summary or rewriting. Its
source-versioned cache excludes results from the former generated-summary format.

Tool headings share the local handwritten font and `static/img/BibleStudyBanner.png`.
Each tool supplies its tint through `--bs-tool-tint` and reuses the `bs-banner`
header style. The navigator always reopens on the current testament's book
list with the current book highlighted; selecting a book opens its chapters.

Thompson margin topics come from explicit verse citations in the numerical
index, with their source PDF pages shown in the popup. The printed book surveys
and charts after the numerical index are not imported as chain-topic articles.
Existing annotation records are preserved against their original editions;
the currently disabled Bible annotation toolbar remains pending integration.

## Mobile reading and personal Bible saves

Bible Study uses a compact shared header, a reading toolbar, and independently
scrolling Bible and study panels. Bible / Book Study tabs are retained. The
horizontal mobile divider resizes the panels; desktop panels remain side by side.
Reading settings (gear) contain paper color, font size, margin references, layout,
chapter saves and links to global notes/highlights and bookmarks. Sync controls
remain developer-only inside settings. Fullscreen layouts expand the chosen
panel while keeping passage navigation and settings accessible.

Tap a word for study information. Long-press and select text for Copy, colored
Highlight, Underline or Note. Tap a verse number for whole-verse actions, including
Bookmark. Selection suppresses word lookup and chapter swipes. Copy includes the
Bible reference; when local Wi-Fi HTTP restricts clipboard access, the reader
provides selectable text for manual copying.

Personal saves use the existing private Bible annotation/bookmark tables. Offsets
are Unicode code points tied to the original verse and edition. Multi-verse notes
are one group. My Library merges these with ebook saves without copying Bible
records into the ebook database. Reads and writes are account-scoped; Bible
writes require a session CSRF token and edits reject stale note versions. Saved
links reopen the anchored passage; an unavailable old edition is identified rather
than silently applying its annotation to new wording. No schema migration or
Bible/AI reindex is needed for this update.

For a local phone preview, keep the phone and computer on the same Wi-Fi and run
from the project directory (port 5001 avoids the Docker app on port 5000):

```powershell
.\.venv\Scripts\python.exe -m flask --app app run --host 0.0.0.0 --port 5001 --no-debugger --no-reload
```

Open `http://COMPUTER_WIFI_IP:5001/pastor-resources/bible-study` on the phone and
sign in normally. Allow Python on Windows Private networks if prompted. This
local preview is separate from the published Docker image.

## Chapter sermons and study-source saves

Sermon Outlines is the fourth study tab. It follows the current Bible book and
chapter, independently of word/verse selection. Results contain original titles
only. Opening a title shows the full indexed outline or teaching section, author,
source, printed passage(s), source page/section and stated occasion/audience.
Sermons whose main passage spans chapters can appear in each covered chapter;
incidental citations in later exposition do not establish coverage. Entries without
an identifiable opening/main passage are omitted from automatic suggestions.
Duplicate PDF/EPUB or locked/unlocked copies are collapsed. The Sermon Outline
Bible Commentary is always sorted after all other sources. The catalogue is
cached by indexed document fingerprints and reads the existing AI database only.

Eligible collections include APEO sermon resources and character/book series,
funeral collections, Joy (Philippians), Rob Westbrook, Spurgeon and suitable
Shepherd's Staff teaching sections. Shepherd's Staff entries identify their teaching
purpose and Ralph Mahoney's editorial credit; they are not labelled as newly
written sermons. Full text means the complete indexed original entry.

Selection actions now work on study text as well as Scripture, including Word
Study, Concordance, Commentary, Book Study, sermon entries and topic/verse popups.
Study pages support image bookmarks and notes. Saves use signed source hashes,
Unicode code-point offsets and retained source snapshots, with per-account
ownership, CSRF protection and note-edit version checks. Global collections merge
these records with ebook/Bible saves. A source change preserves the saved excerpt
and identifies the change instead of silently moving the highlight to new wording.
The Pij website manual describes these controls and their role restrictions.

Before the next Docker update, retain the existing data volume and initialize the
additive study-save table once. This command first creates a SQLite recovery backup
when the table is missing, then retains all Bible editions and personal records:

```powershell
docker compose exec district4 python bible_study.py init-study-saves --database /data/bible_study_data/bible_study.db
```

The local workspace has already received this additive initialization and its
recovery backup is in `bible_study_data/backups/`. Ordinary reading requests never
create or migrate this table. Developer Bible Study sync also carries the table
forward. No ebook reindexing or source database replacement is required.

Bible touch swipes accept shorter, slower, mildly diagonal gestures. Horizontal
intent is detected before suppressing browser scrolling, and touch events handle
mobile browsers that cancel pointer events. Vertical scrolling, multi-touch,
long-press selection and chapter buttons remain available. Chapter changes give
brief passage feedback.


Bible Study source additions (October 21, evening)
------------------------------------------------

The commentary selector now includes The Moody Bible Commentary. Moody uses
book boundaries and paragraph-leading passage references, including single-chapter
books. Sermon Outlines also includes Homiletics and Homilies from The Pulpit
Commentary Complete Volumes 1?9; Exposition text is excluded. Results still use
book/chapter coverage and The Sermon Outline Bible Commentary remains last.
APEO uses its printed theme and opening foundational passage, with passage styling
inside the reader; later supporting references do not determine chapter coverage.

Original source emphasis is stored in the additive `pij_library_source_formats`
table in ai_index.db. Only safe strong/em/sup/sub text runs are sent to readers;
source HTML, scripts and styles are never injected. EPUB paragraphs and emphasis
are extracted from tags/stylesheets; PDF emphasis comes from font metadata.
`fonttools` is required for embedded PDF font encodings. Ordinary reading never
downloads originals or migrates databases. New indexed eligible books prepare
formatting during indexing. Prepare existing editions once after deployment:

```powershell
python -c "import app; import pij_library_knowledge as k; k.refresh_study_source_formats()"
```

Sync Books now removes missing Drive sources from the current catalog, public AI
documents, chunks, search index and source-format records. Cleanup is inside the
successful full-scan transaction; Drive errors and cancellations do not trigger it,
and a cancellation during cleanup rolls both databases back. Hidden or duplicate
files still present in Drive are retained. Private sermon collections are untouched.
Notes, highlights, bookmarks and reading history remain; minimal inactive book
metadata is retained where personal records need its title. Deleted text pages are
freed for SQLite reuse; sync does not run a blocking VACUUM or promise a smaller
physical database file. Retain consistent SQLite backups before the first cleanup.
Commentary/sermon caches include source-format fingerprints and filter current
active, visible catalog sources so stale index rows cannot reappear in results.


Spurgeon outline formatting (October 21, final evening update)
-----------------------------------------------------------

Spurgeon's Sermon Outlines now uses the prepared original EPUB, including actual
paragraph boundaries, bold Roman numeral headings, italic quotations/subpoints and
nested bullet/numbered lists. Source list numbering and item continuations are kept;
only safe text/structure metadata reaches the DOM. A saved source opens with the
same hierarchy, and reading actions continue to anchor to the exact source text.
The author/editor attribution follows the printed edition. Roman-numbered Bible
book citations (I Samuel, II Chronicles, etc.) are recognized without changing their
printed text or Unicode offsets.

The existing `refresh_study_source_formats()` maintenance command prepares this
edition as source-format version 2. Previously prepared Moody/Pulpit/APEO records
remain valid and do not require reprocessing. Ordinary readers do not download or
extract the original books. The local EPUB and PDF have already been prepared.


Pulpit chapter coverage correction (October 22)
---------------------------------------------

Pulpit parsing accepts both separate passage/title lines and combined headings
such as `1Ti. 2:1-15.?Public worship.`. It joins wrapped titles and passage lists,
uses explicit source chapter context for relative verse headings, and recognizes
normal-weight titles when the printed citation identifies a heading. Exposition
and incidental body citations remain excluded from sermon discovery. Complete
article text, source attribution and available emphasis are retained.

The citation vocabulary includes Rth., Son., Luk. and Joh.; Roman book aliases
cannot overwrite existing canonical abbreviations (in particular Isa. remains
Isaiah). In this edition's Ecclesiastes sections, the misprinted `Est.` heading
label is resolved only using explicit Ecclesiastes chapter context; Esther outside
that context keeps its meaning. Homilies also match the explicit source chapter
where they are printed, without rewriting source passage text. This covers the
Exodus 36 homilies whose printed reference incorrectly says Exodus 35.

All nine available volumes were checked against their explicit source chapter
headings. 1 Timothy now has Pulpit suggestions in each of its six chapters.
The correction uses existing prepared/indexed data and needs no Drive resync,
reindexing or schema migration. Restart the application to replace in-memory
catalogues from the previous parser. Saved source snapshots remain preserved.

Pulpit commentary and 52 Sermons typography (October 22)
------------------------------------------------------

The Commentary selector includes The Pulpit Commentary with Exposition and
Homiletics choices, following the selected passage. Homilies remain available in
Sermon Outlines. Verse sections retain continuation pages and source emphasis.
Greek terms open surrounding original source text, supporting selection and
shared reading actions. An optional Word Study link requires an unambiguous
original Greek spelling match in the selected passage plus an existing dictionary
entry; it never infers NIV alignment or generates a definition.

Original EPUB typography is prepared for 52 Sermons From the Gospel of John,
52 Sermons From the Book of Acts, 52 Sermons About Jesus and 52 Funeral Sermons.
The reader keeps paragraphs, bold printed headings/numbering, italic quotations,
whole-chapter preaching references, part numbers and the John bonus sermon.
Publisher advertisements and closing contents are excluded from the final sermon.
If a message has no opening preaching text, its first printed outline quotation
supplies the passage; references in its introductory illustration do not.

On the deployment index, run the existing refresh_study_source_formats maintenance
after transferring/syncing the ebooks; local source-format records are already
prepared. Ordinary readers never download originals. No schema migration is
needed. Restart the application to refresh the cached catalogues; static asset
version study-outlines-15 invalidates the old reader script/style. The compact
selector labels are Moody and The Pulpit; full source names stay in attribution.
