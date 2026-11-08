# Sermon Notes: Drive and Apps Script setup

Bible Study includes a Sermon Notes editor, All Notes, PDF export with download
and sharing, two-way Sync Notes, and the developer's private PDF converter.

## Using Sermon Notes

### Offline Android app login and backup

The website includes `/api/v1/bible-mobile` for IOWO D4 Bible Study. Publish the
updated website image, then update the running server with its existing Compose
project and data volume:

If the deployment's `.env` pins `IMAGE_TAG`, set it to the new release tag first
(or `latest` to follow published updates). Pulling a pinned older tag keeps the
older website code.

```powershell
docker compose pull district4
docker compose up -d --no-deps district4
```

The first app login uses the existing Sheets-backed account cache. Subsequent
requests use a revocable device token; server storage retains its SHA-256 hash,
not the raw token. The phone never receives the shared Apps Script API token.
Each token can access only that account's sermon notes. Account-role prefixes
match the website, including AO/DO aliases; developer conversion remains private.
Password changes/account removal invalidate online access after the account
cache refreshes. Offline access previously granted on a phone remains available.

Authorization records are stored in `mobile_devices.db` beside the configured
sermon-note database, under the existing private Docker data volume. Do not delete
or replace that volume during an update. Existing notes need no migration and
the existing sermon-notes Apps Script needs no redeployment for this connection.

Device synchronization reuses an active website backup, exchanges the device's
notes with Drive, then refreshes the website again. Finish bulk backups before
updating/restarting the server. Requests without a token are rejected before
Sheets synchronization or browser visit logging.

After the server update, a JSON POST with an empty object to
`/api/v1/bible-mobile/login` should return HTTP 400 with a username/password
validation message. HTTP 404 means the running website still lacks this update.
This check sends no account credentials and does not start a backup.

### Website editor

Open **Bible Study > Sermon Notes**. **Create Notes** first asks for Text and
Theme. Edits save locally after a short pause; use **Save** before leaving if
you want immediate confirmation. A device recovery draft also protects unsaved
edits across an accidental refresh. **All Notes** contains Testament and Book
filters, full-content search and the **For Review** group. Correct Text and
Theme to move a sermon into canonical order.

The small, plain header shows Theme and the passage reference only. The actual
document starts with formatted, editable Theme and Text paragraphs. Verse
wording belongs in the document body. The font toolbar provides family, point
size, grow/shrink, case, bold, italic, underline, highlight and text colors;
the former paragraph-style menu is removed. These styles survive saving,
reading, Drive synchronization and PDF export (with substitute PDF fonts when
the source family is unavailable).

On phones and touch-enabled tablets, **Editing tools** opens these controls in
a pop-up. It closes after an edit/format action, an outside tap, Escape, or
scrolling the reading pane. The desktop keeps the visible sticky toolbar.
Delete first names the target sermon in a confirmation with **Cancel** and
**Delete sermon**. Cancel has default focus; closing or dismissing that dialog
does not send a delete request.

**Read Notes** keeps formatting and makes detected references clickable. Verse
references are also offered below the live editor. The popup's insert button
adds the passage body without its translation/attribution label. **Current
notes** sends selected material or a whole source to the currently open sermon;
otherwise a chooser offers new or existing notes. An insertion uses the remembered
cursor position, without replacing an earlier text selection. Undo removes it.

**Export Notes** first prepares a PDF, then offers Download PDF or Share PDF.
Browsers without file sharing download the PDF instead, like Pastor's Tool.
The options menu uses content-sized rows on phones. Native touch activation
keeps the menu open through Safari's focus change until the chosen Export,
Sync or Convert action runs; outside taps and keyboard dismissal still close it.
PDFs embed bundled Noto Sans fonts for Latin and Greek text, including bold and
italic. The unmodified fonts and SIL Open Font License are under `static/fonts`,
from [notofonts/noto-fonts, commit ffebf8c1ee449e544955a7e813c54f9b73848eac](https://github.com/notofonts/noto-fonts/tree/ffebf8c1ee449e544955a7e813c54f9b73848eac/hinted/ttf/NotoSans).

**Sync Notes** pulls and pushes. Use it on a new installation to restore your
library. A conflict keeps local edits and the Drive revision: **Keep both copies**
saves the local edit as a separate sermon and restores the Drive version;
**Use Drive version** discards the conflicting local copy after confirmation.
Concurrent edits in another browser tab are preserved as a separate local copy.

Only the existing developer account (Pijeme, Area Overseer) sees **Convert my
sermons**. First sync the existing Sermon eBooks catalog. Conversion imports its
active PDFs into that developer's personal notes, preserving text, emphasis,
color, size, indentation and page boundaries where extractable. Converter v2
groups glyphs by their printed baseline, keeping verse numbers and footnote
markers inline. It separates references from verse quotations in Text headers,
including chapter-only references and references after a quotation. This fix is
part of `bible_study.py`, not a local database-only correction: deploying the
updated code and using Convert on the main website uses the same converter.

Re-running Convert repairs older imports. Before its first repair it creates a
consistent SQLite snapshot in `backups/` beside `sermon_notes.db`. Unchanged
text is converted again and manual inline formatting is transferred; authored
text changes are kept rather than replaced from the PDF. If a note changes
while the repair is running, the update is skipped and reported for retry.
Already-current imports, locally deleted notes and unresolved conflicts are
skipped. Repairs preserve IDs and remote versions and mark the local changes
for the next Sync Notes. No Apps Script update is needed for this converter fix.
Unreadable/scanned files are listed for attention and need OCR; PDF images and
exact original pagination/font families are not recreated in editable notes.
The source files are kept unchanged. Conversion itself does not upload notes;
use Sync Notes afterward.

Personal storage is `sermon_notes.db` beside the Bible Study database by default,
or the absolute path in `BIBLE_SERMON_NOTES_DATABASE`. In the supplied Docker
configuration this is `/data/bible_study_data/sermon_notes.db`, inside the existing
persistent volume. It is separate from indexed Bible content and is created on
the first save/sync/conversion, never by ordinary reader requests. No new Python
module or dependency is required. Restart the app (or recreate its container)
after updating code or environment settings. Existing Apps Script code remains
compatible; no redeployment is required for this app integration.

## Storage choice

Use the website's local database for editing and JSON on Drive for portable
backup. JSON retains structured paragraphs, nested points, bold, italic,
highlights, verse text and import layout metadata. PDF is a separate export.
The bridge preserves JSON as data. The editor validates structured text/styles
and renders text through DOM nodes; imported HTML is not executed.

Destination: [the supplied Drive folder](https://drive.google.com/drive/folders/1E-bNrscGBsddTljXDqGlOwk0YaAoIZEG).

```text
Supplied folder/
  users/
    username--SHA256-of-account-key/     (created on the first backup attempt)
      index.json                       (current notes and deletion markers)
      revisions/
        noteid-v1-requestid.json
        noteid-v2-requestid.json
```

User folders are automatic; no separate Google account or deployment is needed
for each website user. The website supplies its existing normalized private
reading account key, such as `pastor:username`, from the authenticated session.
Role names with spaces are URL-encoded, for example
`area%20overseer:pijeme`, to match the deployed bridge's key rules while leaving
the username readable in its folder. This encoding is stable across devices.
The full key hash distinguishes roles and usernames that sanitize to the same
folder label. Keep the same account key on every device/server; a username or
role change needs a deliberate account migration. Never let the browser choose
another account's key, and never use a display name as the identity.

The Drive owner can access all backups. Folder separation enforces no Drive
sharing policy by itself: keep the parent folder Restricted and grant access
only to intended administrators. The script never makes folders or files public.
App users access their own notes through authenticated website routes, not
through shared Drive links. Existing inherited Drive permissions still apply.

## Deploy under the folder owner's Google account

1. Open [Google Apps Script](https://script.google.com/) as the destination
   folder's owner. Create a **new project**, named `District 4 Sermon Notes`.
   Keep the existing CCM, Event and Pledges projects separate.
2. Replace that new project's `Code.gs` with all of
   [sermon_notes_drive.gs](sermon_notes_drive.gs). Its destination ID is already
   set to `1E-bNrscGBsddTljXDqGlOwk0YaAoIZEG`.
3. Run `setupSermonNotesDrive` from the editor and authorize Drive access.
   It creates `users` if missing and prints `SERMON_NOTES_DRIVE_API_TOKEN` in
   the execution log. Save that token privately in the server's `.env`.
   Setup is repeatable and preserves the token and all existing backups.
4. Run `verifySermonNotesDrive`. It creates, reads and updates a synthetic JSON
   file in a temporary check folder, then moves that check folder to Trash.
   A `PASS` log confirms actual write access. It does not read personal sermons.
   This checks Drive directly; it is not a deployed-URL test.
5. Select **Deploy > New deployment > Web app**. Set **Execute as: Me** and
   **Who has access: Anyone**. The private token authenticates server requests;
   the deployment URL alone does not authorize access to notes. If your Google
   organization disallows this access setting, this deployment method will
   need to be changed before server integration.
6. Copy the deployed URL ending in `/exec` into the server's `.env`:

   ```dotenv
   SERMON_NOTES_DRIVE_SCRIPT_URL=https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec
   SERMON_NOTES_DRIVE_API_TOKEN=YOUR_PRIVATE_GENERATED_TOKEN
   ```

The app reads these variables on startup. Keep the token out of chat, source control, browser JavaScript,
query strings and shared screenshots. Requests must come from the website
server. Neither a website password nor a Google service-account credential is
the bridge token. Owner execution uses the owner's Drive access and storage.

Updating code later requires **Deploy > Manage deployments > Edit > New
version > Deploy** so the existing `/exec` URL serves the new code. Re-running
setup alone does not update an existing deployment. For token rotation, change
the script property and server environment together; setup does not rotate it.

## Verify the deployed endpoint

Opening the URL in a browser should return `POST_REQUIRED`, never note data.
The following optional check sends only a status request from your computer.
It prompts for the URL and private token and prints no token. It does not create
user folders or notes. It requires the project's installed `requests` package.

```python
import getpass
import requests

url = input('Deployed /exec URL: ').strip()
token = getpass.getpass('Private token: ')
response = requests.post(url, json={
    'purpose': 'sermon_notes', 'action': 'status', 'token': token,
}, timeout=60)
response.raise_for_status()
result = response.json()
print({key: result.get(key) for key in ('ok', 'code', 'schema_version', 'folder_id')})
```

Expect `ok: true` and the supplied folder ID. Apps Script JSON replies use an
`ok`/`code` body, including failures; HTTP 200 alone is not proof of success.
An HTML sign-in page usually means deployment access settings or the URL need
attention. `AUTH` means the configured tokens differ or setup has not run.
`STORAGE` can indicate access, quota, duplicate files/folders or damaged JSON;
do not delete backups to resolve it. A missing folder/index on a legitimate new
account reads as an empty library; an unreadable existing index fails safely.

## Server protocol

All operations are JSON POSTs to `/exec` with `token`, `purpose: "sermon_notes"`
and `action`. Other account operations also require `user_key`. All IDs below
are lowercase 32-character hexadecimal UUIDs (without hyphens).

| Action | Additional fields | Result |
| --- | --- | --- |
| `status` | None | Destination access and protocol version; token required |
| `list` | Optional `limit` (1-100), `after`, `index_version` | Summaries including deletion markers, index version and `next_after` |
| `get` | `note_id`, optional expected `version` | Current full revision, or `CHANGED` if the requested version moved |
| `put` | `note_id`, `base_version`, `request_id`, `note` | Creates/updates only the expected revision |
| `delete` | `note_id`, `base_version`, `request_id` | Writes a deletion marker; retains earlier revisions |

`note` is a JSON object with `schema_version: 1`, nonempty `theme` and `text`
(maximum 500 characters each), and a `content` object. Additional metadata is
preserved unchanged. The bridge treats Text as the user's passage label, not
as verse body text. Imported sermons must have a nonempty provisional Text
(for example `For Review`) if their main passage is uncertain; retain the
original text and theme alongside editable corrected values. Example content:

```json
{
  "schema_version": 1,
  "theme": "THE BIRTH OF A VISION",
  "text": "Matthew 9:35-38",
  "primary_reference": {"book_id": 40, "chapter": 9, "verse": 35, "end_chapter": 9, "end_verse": 38},
  "needs_review": false,
  "content": {
    "blocks": [
      {"type": "main_point", "runs": [{"text": "I - WHY SPIRITUAL VISION?", "bold": true}]},
      {"type": "paragraph", "runs": [{"text": "Supporting thought", "italic": true, "highlight": "#ffe66d"}]}
    ]
  }
}
```

The editor stores structured blocks with rich text runs, indentation, font size,
color and page-boundary hints. The bridge keeps this JSON unchanged. The server
revalidates it before editing and rendering. Original PDFs stay unchanged.
The server validates Scripture references, conversion permission, content shape
and size, and attaches the authenticated account key itself.

Each note is limited to 512 KiB of UTF-8 JSON, each request to 1 MiB and each
account index to 4 MiB. Send one note at a time and batch work on the server;
never upload all 500+ sermons in one Apps Script execution. Images/PDF binaries
are not part of this bridge. No automatic timers or public file URLs are created.

### Safe two-way sync

1. Fetch every page of `list`, keeping the first returned `index_version` on
   subsequent pages. Restart the listing on `CURSOR_EXPIRED`. On a new device,
   download live notes with `get`, persist them locally, and retain their remote
   versions. If `get` returns `CHANGED`, refresh rather than ignore the change.
2. Fetch remote changes before pushing local changes. A missing local note is
   never an instruction to delete a remote note. Apply deletion markers to
   unchanged local copies; if that note has local edits, preserve it for conflict
   resolution rather than discard the edit or resurrect the deleted remote note.
3. Push a changed note with the last acknowledged remote `base_version` (zero
   only for a brand-new note ID). Use a fresh `request_id` for each intended
   mutation and retain the exact request for retry after a timeout. Confirm the
   response before marking a local edit backed up; newer local edits stay dirty.
4. `CONFLICT` means another revision won. Preserve local work, fetch the remote
   copy and let the user resolve it or save a separate copy. Do not blindly
   overwrite it with a new base version. `REQUEST_REUSED` means a request ID was
   reused with different data. `BUSY`/`STORAGE` may be retried with bounded backoff;
   `AUTH`, validation errors and `INDEX_FULL` require attention first.
5. Deletions are explicit, versioned operations. Keep their markers so an older
   device cannot restore removed notes accidentally. Historical revision files
   are retained, but there is no history browser or automatic pruning yet.

Writes first save an immutable revision, then update the account's index under
an Apps Script lock. Until that index is published, the previous index remains
authoritative; a retry of the same request can finish the staged revision. If a
timeout follows a successful publication, the retry acknowledges that revision
without creating another one.
Staged unreferenced files are not visible notes and must not be blindly imported.
Do not manually edit, rename, duplicate or move index/revision files. One Apps
Script project/deployment must own this folder: script locks do not coordinate
independent projects pointing to the same destination. Several devices or app
servers can use that one endpoint.

## Editor behavior

- Add **Sermon Notes** beside **Bible** and **Book Study**. Its controls are
  **Create Notes**, **All Notes**, **Export Notes**, **Sync Notes**. Testament,
  book and search filters belong inside **All Notes**.
- Creation first asks for **Text** and **Theme** and inserts them as formatted
  content. Editing uses the Word-style font controls. Imported Roman numeral,
  lettered and numbered outlines retain their source formatting.
- Recognized references open a verse popup. Inserting a verse inserts its full
  body without translation names or attribution labels in the sermon body.
- **Export Notes** creates a PDF and then offers local download or sharing,
  following the existing Pastor's Tool report flow.
- **Sync Notes** pulls and pushes, including restoration for a new device.
  Automatic Drive folders belong to each logged-in user's personal library.
  If Drive's index or an entry was removed externally, sync backs up the current
  local sermons again after Drive confirms the missing entry. Unsynced newer
  edits are preserved, and local deletions never recreate missing sermons.
  Different existing Drive revisions still require conflict review.
- Selection and whole-entry insertion from Bible, commentary, illustrations,
  outlines and saved notes use **Current notes** as the checkbox label. Checked
  means the sermon currently open; unchecked opens Create New / Add to Existing.
  Without an open sermon, show the chooser even if the preference was checked.
- Only the developer account gets **Convert**. Its 500+ converted sermons belong
  exclusively to that developer's personal account, never to a shared collection.
  Preserve PDF formatting as closely as extraction allows, retain source links,
  and place sermons with unresolved primary passages in **For Review**, where
  Text and Theme can be corrected manually. Preserve manual edits on reimport,
  and repair older converter versions after making a backup.

## Google references

- [Web app deployment and execution identity](https://developers.google.com/apps-script/guides/web)
- [Drive file operations](https://developers.google.com/apps-script/reference/drive/file)
- [Script locking](https://developers.google.com/apps-script/reference/lock/lock-service)
- [Apps Script quotas](https://developers.google.com/apps-script/guides/services/quotas)
