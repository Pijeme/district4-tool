# CCM

The public page is `/ccm`, with About CCM, Workers, and Admin tabs. Its menu entry
is directly below Event Registration. The login page also links to CCM.

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

## Worker and ID behavior

- **Create Account** is the first admin choice; **Edit / Delete Account** is the
  second. Area and church selections use existing district Accounts data; the
  pastor is resolved on the server. Names from Members Account are suggestions.
- Worker name, area, church ID/address, pastor, campus, phone, email, status,
  ID number and creation/update timestamps appear in the public directory.
  District passwords and birthdays are never copied into CCM records.
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
- Delete removes the account and invalidates its QR immediately on this host.
  Its next backup marks the Google row `Deleted`; its sheet record and Drive
  files remain as ID history. Pending deletions are included in the backup count.
- QR verification uses pending local changes first. For backed-up records it
  checks Google and never attests validity from a stale cached record.

## Artwork and printing

The original JPGs are copied byte-for-byte to `static/ccm/id-front-template.jpg`
and `static/ccm/id-back-template.jpg`. The renderer in `app.py` overlays only
the portrait, worker name/title, ID number, actual issue date, QR and requested
contact details. The original border artwork, logos, criteria and signature
areas are retained. Both output images are 1050 × 1500 PNGs.

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
