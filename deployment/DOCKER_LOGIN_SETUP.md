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

The Maxwell bulletin insight uses a separate white reading panel. Its short
summary is generated from the matched indexed article; Read more displays that
article's indexed wording and paragraph breaks. The daily cache key includes
the source fingerprint and the current result format so prior generated
expansions cannot appear as original source text.

Tool headings share the local handwritten font and `static/img/BibleStudyBanner.png`.
Each tool supplies its tint through `--bs-tool-tint` and reuses the `bs-banner`
header style. The navigator always reopens on the current testament's book
list with the current book highlighted; selecting a book opens its chapters.

Thompson margin topics come from explicit verse citations in the numerical
index, with their source PDF pages shown in the popup. The printed book surveys
and charts after the numerical index are not imported as chain-topic articles.
Existing annotation records are preserved against their original editions;
the currently disabled Bible annotation toolbar remains pending integration.
