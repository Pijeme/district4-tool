# CCM

The public page is `/ccm`, with About CCM, Workers, and Admin tabs. Its menu entry
is directly below Event Registration in the main signed-in menu.

The CCM link is available in the main signed-in menu; Event pages and the login
splash do not show a CCM shortcut.

The admin password is `CCMDistric4`, checked only on the server. Admin sessions
last four hours. Admin actions require a session and CSRF token; the shared
password is never embedded in HTML or JavaScript.

## Google storage setup

The existing Google service account can read/write `District4 Data`. The new
`CCM Workers` worksheet has already been created. The application also creates
it on a new installation. Accounts save locally first. An explicit backup
copies their records, ID numbers, creation/update timestamps, verification links
and the three verified Drive file IDs into this worksheet.

The supplied folder is `1IYlGZPuHL7CUW7HqwWEHnxuaG63gscEY` (`CCM Profile Pic`).
The current `deployment/ccm_drive.gs` is **CCM-only**. It accepts the existing
CCM portrait/front/back payloads and rejects Event purpose, event keys and
custom filenames before accessing Drive. It preserves the existing private
token and CCM filenames. Publishing it does not rename or move existing files.
Event backups need their own upload endpoint; until the website is connected to
that endpoint, Event requests sent to this script will fail safely and stay local.
Access to the folder is confirmed, but a real PNG upload returns Google's
`storageQuotaExceeded`: service accounts cannot use personal Drive storage.
An empty-file permission check can succeed despite this limitation.

For this personal Drive folder, deploy the supplied upload script under the
folder owner's account:

1. Open Google Apps Script while signed in as the folder owner and create a new
   project. Paste the complete contents of `deployment/ccm_drive.gs` into its
   script file. This is a separate project; do not replace the pledges script.
2. Run `setupCCMDrive` and authorize Google Drive access. Copy the generated
   `CCM_DRIVE_API_TOKEN` from the execution log into the website server's `.env`.
   Re-running setup keeps the same token. Do not use the admin password as this
   API token.
3. Deploy as **Web app**, **Execute as: Me**, **Who has access: Anyone**. The
   long private API token authenticates the website's uploads.
4. Copy its deployed `/exec` URL into `CCM_DRIVE_SCRIPT_URL` in the same `.env`.
5. Keep the folder readable by the website's existing service account, and
   restart the local app. For Docker Compose, run
   `docker compose up -d --force-recreate district4` so the container receives
   the updated environment variables from `.env`.

```dotenv
CCM_DRIVE_SCRIPT_URL=https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec
CCM_DRIVE_API_TOKEN=YOUR_GENERATED_PRIVATE_TOKEN
```

Uploads then use the owner's storage. Reading images and updating Sheets still
use the existing service account. The browser receives neither Google's
credentials nor the upload token. Retries use content-addressed filenames and
reuse completed uploads. If using a Workspace Shared Drive instead, direct API
uploads are available without the bridge; the current folder constant would
need to point to that Shared Drive's folder.

### When neither Drive nor Sheets backup completes

The Apps Script only uploads images; the website writes Sheets after it verifies
those uploads. A Drive failure stops that record before its Sheets write and
keeps its local form and images for retry.

If the message mentions a service-account upload or connecting the folder
owner's account, the running app is not using the Apps Script upload connection.
Check **the deployment's** `.env`, beside the Compose file used to run the server;
editing a separate development checkout's `.env` does not change that container.
Both `CCM_DRIVE_SCRIPT_URL` and `CCM_DRIVE_API_TOKEN` must be present. Recreate the
container with `docker compose up -d --force-recreate district4` from that
deployment directory. A plain container restart does not reload `.env` settings.
Then retry the existing backup; there is no need to register again or change
the Google Sheets columns.

An API token rejection means the server's token differs from the Apps Script
property. A `DRIVE` error means the script was reached but failed while using
Drive; inspect its private Apps Script Executions log and verify the folder
owner's authorization and storage. Keep tokens and credentials out of screenshots.

## Worker and ID behavior

- **Create Account** is the first admin choice; **Edit / Delete Account** is the
  second. Area and church selections use existing district Accounts data; the
  pastor is resolved on the server. Names from Members Account are suggestions.
- Worker name, area, church ID/address, pastor, campus, phone, email, status,
  ID number, expiration date and creation/update timestamps appear in the public directory.
  District passwords and birthdays are never copied into CCM records.
- Expiration is set automatically to one calendar year after the original ID
  creation date in Philippine time. February 29 expires on February 28 of the
  following year. The form displays this date read-only; posted date overrides
  are ignored. It is printed in the front's **Valid Until** field and saved with
  the worker locally and in Google. The ID remains valid through that date;
  the directory and QR verification show **Expired** afterward. Editing an
  account calculates the date from its original creation, without extending it.
  Existing accounts retain their saved validity until edited and saved; saving
  applies the automatic date and regenerates both PNGs for printing and backup.
- The application appends **Expiration Date** as column R when it encounters the
  original 17-column `CCM Workers` worksheet. Existing columns and rows stay in
  place; other header changes are rejected. No manual worksheet migration is needed.
- Registration writes account data into `ccm_pending_workers` in the existing
  app database and writes immutable PNGs into `DATA_DIR/ccm_assets`. There are
  no Google requests during a save. "Local" means the computer/server running
  the website, not the phone. Keep the database and image directory together.
- Large phone photos are resized to a maximum dimension of 1400 pixels in the
  browser before upload. The server also downsamples supported originals up to
  80 megapixels / 12 MB; phone capture and browser preparation normally send
  much smaller files. ID downloads remain separate PNGs.
- The camera selector offers **Front camera** and **Back camera**, including
  switching during a live preview. Only the front preview is mirrored; captured
  files keep their normal orientation. The last camera choice is remembered by
  the browser for that website address.
- After saving, **Create New**, **Print Now**, **Print Later**, and **Exit** are
  available. Exit locks the CCM admin session. Local accounts survive app
  restarts and can be printed or edited before backup.
- **Back Up to Google** processes the pending collection one account at a time.
  Its percentage represents accounts fully verified and cleaned up, with an
  explicit completed/total counter. The three image uploads run concurrently.
  Each Drive file's size and MD5 checksum are checked, and all Sheets fields
  are read back and compared before local image files are removed.
- Backup also commits the Google directory index locally before removing the
  pending record. Workers always merge local changes over that index. After
  backup, portraits and IDs are read from Drive, using a bounded memory cache;
  reading them does not recreate local image files. **Refresh Google accounts**
  imports remote changes explicitly. A new installation imports Google once
  when no local or cached directory is available.
- Failed uploads, unconfirmed Sheets writes, and mismatched checksums keep the
  local source for retry. Lost acknowledgements reuse the same record key and
  content-addressed files. If cleanup is interrupted after confirmation, saved
  cloud references allow a retry without uploading again. An account edited
  during backup remains pending, with all its newer local images preserved.
- Editing preserves the original ID number, issue date and verification link.
  Its next backup updates the same Google row. Stale local edits and conflicting
  Google revisions are rejected. Older Drive versions remain as history.
- If a Google row is missing, **Backup to Google** restores that pending account
  with its original record key and ID number. Existing Drive images are verified
  before restoring the row; readable images in Drive's trash are verified and
  backed up again through the owner upload connection. Duplicate ID numbers are
  rejected. Local data is cleared only after Google confirms the full restored row.
  Pending deletions write a `Deleted` history row without restoring trashed images
  or reactivating the account.
- Delete immediately removes the local profile cache and local photo/ID PNGs,
  and invalidates the QR on this host. Only a small deletion marker stays pending
  until Google confirms it. The next backup marks the Google row `Deleted` and
  removes that marker; existing sheet history and Drive references are preserved.
  Pending deletions are included in the backup count.
- QR verification uses pending local changes first. For backed-up records it
  checks Google and never attests validity from a stale cached record.

## Artwork and printing

The original JPGs are copied byte-for-byte to `static/ccm/id-front-template.jpg`
and `static/ccm/id-back-template.jpg`. The renderer in `app.py` overlays only
the portrait, worker name/title, ID number, actual issue date, expiration date,
QR and requested contact details, including `campuschristianmovement2c518@gmail.com`
in place of the printed website/email. The QR verification URL is unchanged.
The back includes **Cosme Walawala — District
Overseer** and **Ferlor Anger Rudrigues — Campus Ministry Overseer** with their supplied
signatures. `overseer-signatures.png` preserves the supplied transparent image:
the bottom signature belongs to Cosme and the top to Ferlor. The holder's
signature space remains blank. The original border artwork, logos and criteria
are retained. Both output images are 1050 × 1500 PNGs.

Printing starts with the back. **Print back again** and **Next: front** let the
admin inspect the result before changing sides. Only the selected side appears
in the print document; advancing is always manual, even when printing is
cancelled. Front and back have separate PNG download buttons.

The default physical print size is 70 × 100 mm to retain the supplied artwork's
aspect ratio. Admins can adjust the width; height follows the original ratio.
Use actual size / 100% in the printer dialog, and verify card orientation for
the particular printer. No physical printer has been tested in this workspace.

The PDF is copied unchanged to `static/ccm/CCM Manual of Operation.pdf`.
`manual.json` contains its extracted text verbatim; the About tab shows exact
introduction/vision/mission passages and expandable full chapters. The original
PDF includes the diagrams and original pagination. `Tinos-Bold.ttf` supplies a
portable Times-style font; its license is included beside it.
