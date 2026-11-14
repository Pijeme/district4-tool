# Event Registration

The shared public link is `/event-registration`. Its Registration tab uses the
one active event. The Admin tab uses the server-side password `EventAdminD4`
and a four-hour session. Only the signed-in developer account, verified against
the local Accounts cache, can create or edit an event. Event admin access alone
does not grant that permission.

## Events and registration

Public registration opens on **November 15, 2026, at 6:00 PM Philippine time**
(`2026-11-15T10:00:00Z`). Until then, the Registration tab shows the supplied
convention poster and a countdown. The server blocks early detail submissions,
camera uploads, review pages/previews and registration confirmation, including
previously saved draft links. Registered Attendees and admin login remain accessible.
The signed-in developer account (validated against the Accounts mirror) and an
unlocked event-admin session are exempt. Other account roles are not exempt.
The countdown uses server time and verifies the opening before automatically
loading the form. The opening check needs no scheduled job and runs after a restart.
Deploy the updated code and `static/event/general-convention-registration-opening.jpg`
before the opening; local changes do not update the hosted website automatically.
After this one-time launch date, registration works normally for future events.

The developer's Create Event dialog offers District Convention and Youth
Convention, start/end dates, a theme, Bible text/reference and a finished ID
template picture upload. The start year forms the stored Event value, for example
`District Convention 2026`. Preview ID renders the actual layout with sample
attendee details. Theme and Bible reference are event metadata for the registration
page; the finished ID artwork already contains the fixed lettering. Only the photo,
name, area, church and QR are added to the template. Saving checks that attendee
text fits at readable font sizes.
Creating a new event makes it active and preserves all earlier events and
attendees. The admin event dropdown can view earlier collections. An event
with registrations must keep its type and year; dates, theme, text and
background can still be edited. Changes to the design apply to future prints.

The public form collects Name, Bday, optional Contact #, Area, Church Name,
Church Address and Pastor. Area and church selections come from cached
Accounts data. The server fills address/pastor; New church permits manual
entry without adding an Accounts row. Area 0 is excluded from the choices and
rejected by the server.
After choosing an area, **Area Overseer** is the first selectable church option.
It saves that label as the church, uses the area's cached overseer ministry
address/name where available, and applies one `Ptr.` prefix. It does not grant
an account role or administrative access. Details are saved locally before the
separate camera page. A live-camera capture saves a draft photo, then **Review ID**
opens `/event-registration/review/<key>`. The owner-only preview image never
generates a QR, contains repeated watermarks and a non-entry banner baked into
the pixels, and is limited to 600 × 900. The attendee can edit the registration
form while keeping the photo or retake it. Confirmation is a separate CSRF-protected
POST; a revision check rejects stale details/photos. Only confirmation assigns
the attendee number and marks the registration complete. Drafts are excluded from
the roster, scanner, ticket verification and backup. Repeated confirmation does
not allocate another number. The success page asks whether to Register another
(a new blank form) or Exit (the Registered Attendees tab).

There is no photo picker. Cameras require HTTPS or localhost. Camera switching,
capture, preview and retake are supported. Retries reuse the same registration.
Below Review ID, Cancel submission opens a confirmation dialog with
Yes, cancel and Back. Back preserves the captured photo and details. Confirming
removes only that visitor's unfinished local registration and clears its session
ownership. Cancellation requires CSRF and never deletes a completed attendee or
consumes a registration number. Registration and its confirmation make no cloud
uploads; an administrator starts them with Backup Data.
On desktop, choose **Default camera / desktop webcam**, click **Open camera**
and allow browser access. After permission is granted, the Camera list also
shows connected webcams by name. A disconnected camera or unsupported video
constraints fall back to the default webcam. Front/back choices remain available
for phones. If access is blocked, check site camera permissions and the operating
system's camera privacy settings. Capture stops the live stream; Retake opens it
again. HTTP links using a LAN IP address cannot access cameras; use HTTPS or
`http://localhost:5000` on the computer running the app.
Pastors and area overseers receive one `Ptr.` prefix in the saved name, printed
ID and Google export. Matching Accounts names are recognized within their area
and church; the registration checkbox also supports ministers from new churches.
Repeated or already-entered titles are normalized to one `Ptr.`.

Attendee numbers are allocated atomically on completion, starting at 1 for each
event, so unfinished forms do not take registered-attendee numbers. They never
change when sorting, printing or backing up. Date created is the first local
details save, including when the camera step finishes on another day. Internal
opaque keys protect private photos and support stable QR links; these are not
shown as registration numbers on IDs.

The **Registered Attendees** public tab shows a small photo, registration number,
name, area and church for completed registrations in the active event. Public
photo URLs use event ID and attendee number instead of private ticket keys;
the server returns only an 80 × 96 JPEG, with a placeholder when no photo exists.
Full-resolution photos retain their existing access checks. Public thumbnails
stop working when the registration is deleted or its event is inactive. It supports
search, sorting and an area filter without renumbering attendees. The banner
shows the live registered count beside the dates. Neither the list nor the count
includes unfinished camera steps or deleted registrations.

Public and admin pages check `/event-registration/progress` every seven seconds
while visible. SQLite triggers maintain an event version, so unchanged checks
return a small cursor response without reading or sending the roster. Checks
never sync Google, write schema or log visits. Changes update the existing list
without a page reload; search, area, sort and processing stage are preserved.
Refresh checks immediately. Admin access is checked on every private poll.

## Local storage and Google submission

`registration_events`, `event_attendees`, `event_deletion_queue`,
`event_roster_versions` and `event_request_limits` are additive
tables in the existing app database. Pictures are immutable PNGs under
`DATA_DIR/event_assets`. Keep that directory together with the database during
server moves and backups. Local data and images remain after successful cloud
submission. No cloud import replaces local records.

Event generated-ID uploads use their own `EVENT_DRIVE_SCRIPT_URL` and
`EVENT_DRIVE_API_TOKEN`. They never fall back to the CCM endpoint or CCM folder.
Create a **separate Apps Script project** owned by the Event folder owner, paste
`deployment/event_drive.gs`, and run `setupEventDrive`. Copy its generated private
token into the server's `EVENT_DRIVE_API_TOKEN`. Deploy as a Web app executing
as **Me**, with access **Anyone**; put its `/exec` URL in `EVENT_DRIVE_SCRIPT_URL`.
Keep both values separate from the existing CCM settings. Restart the updated
local app or recreate its updated Docker container after setting these values.
The Event folder is fixed to `1pvwfSIS0kcvjp__O9--9-1-2f7zIBcEI`; keep it readable
by the app's service account so uploaded PNGs can be verified. The app checks
the returned Event purpose, folder, image side and filename before accepting an
upload. Missing settings or an incorrect endpoint keep the registration local.
A failed backup keeps registration successful
locally and can be retried by Admin with Backup Data. ID filenames are exactly
`Event>Full Name>10-ID.png` for the tenth attendee; unsafe filename separators are
replaced with underscores. Event/record/content identity distinguishes the
same filename in different events and reuses confirmed retries.

The Event script supports legacy `photo` requests, but the website sends only
`id` uploads named `Event>Full Name>10-ID.png`. It rejects CCM requests and reuses
identical uploads by event, registration, side and content identity, including
when separate events restart attendee numbering at 1. Backup Data uploads only
the actual printable ID, then verifies its Drive size/checksum
before writing the attendee's Sheets row. Portraits stay local for generating and
printing IDs. Deleting an attendee also queues any previously uploaded Event
portraits for removal with their generated IDs.
The additive local `id_drive_id` and
`id_backup_hash` columns record a confirmed ID upload. Unchanged images are reused
on retry; a changed ID rendering (for example after a theme/background edit) is
uploaded as a new version on the next backup. Partial failures retain all local
details and portraits plus confirmed cloud references for retry. Migration of
existing Event files from the former shared folder remains a separate change.

Backup Data processes the selected event one attendee at a time.
It uploads/verifies printable IDs and writes the worksheet named `Event Registration`
in `District4 Data`. Headers must be these exact names, in any order:

`Name | Bday | Contact # | Area | Church Name | Church Address | Pastor | Date created | Paid | Print | Release | Event`

It creates headers only on an empty worksheet. Existing different headers stop
submission rather than being replaced. All cell values use RAW input, including
names that begin with spreadsheet formula characters. Date created is the
original ISO timestamp, not submission time. Status values are `Paid`, `Printed`,
`Released`, or blank. Local values overwrite Google changes on resubmission.
Rows are matched by event and creation timestamp, then the last submitted
identity/saved row. Duplicate/ambiguous matches stop without deleting local
data. Lost acknowledgements are retried against the same row. Changes made
during backup remain local changes until the next submission.

## Admin and printing

The spreadsheet-style list shows number, name, area, church name/address,
Paid, Print, Release and Delete. Birthday, contact, pastor, creation date,
Event and Google-copy fields are kept in local storage and the Sheets export
and are not rendered as table cells or hidden search data. The list searches
the visible attendee details, sorts by
registration order, area, church or name, and filters by area. Paid and Release
have separate actions. Each admin can choose Payment only, Print only, Release
only or All actions; the choice is saved on that browser. Focused views show the
number, name, area, church and selected action. Phones show compact portrait cards
without horizontal scrolling. The existing gear icon beside Create Event and
Lock admin opens an Event & backup dialog containing event selection, event
editing/deletion, Backup Data, cleanup counts and backup progress. Its close
button and Escape are disabled while backup is running.
Revision checks prevent stale windows from overwriting
newer local changes. Admin mutations require CSRF protection.

Delete registration requires confirmation and a current revision. It removes
the local attendee row and revokes their QR, photo and print access immediately; existing
attendee numbers never change or get reused. A queued deletion is retained
in `event_deletion_queue` until Backup Data confirms both the Google Sheets row
removal and Google Drive cleanup. The queue keeps the identifiers needed to retry,
plus the name, area and church labels needed to review the pending list, not the
full registration or its photo. The Event script moves only files
whose descriptions match that exact event and attendee identity to Drive Trash,
including old generated versions and legacy portraits. Other attendees' files
and the folder are preserved. Partial failures retain the queue for retry, and
deleted registrations are never uploaded again. Local immutable image files
remain on disk; they are no longer accessible through the deleted registration.

**Existing installations must redeploy the updated `deployment/event_drive.gs`
as a new version of the existing Event web app.** Keep its URL and token. The new
authenticated `action: delete` operation is required to confirm Drive cleanup;
an older deployment leaves deletions queued instead of reporting false success.

The gear dialog's **Review pending records** opens an admin-only list covering
legacy deleted rows and durable cleanup records from all events. Each entry shows
its name, event, area/church, deletion time and latest error. **Retry Google cleanup**
retries that record through the existing confirmed-deletion flow; it does not
restore the attendee or upload a deleted registration again. **Delete local record**
requires explicit confirmation and only removes that local row/cleanup item,
without calling Google. This is intended for cloud copies the admin already
removed manually. CSRF, revision checks, active-row protection and the per-record
backup lock prevent accidental or concurrent removal. Neither action renumbers
active attendees.

Only the developer can see or invoke Delete event. Each deletion requires entering
the current event admin password, including when Admin is already unlocked. The
server checks it on every request; the dialog clears it after confirmation,
Cancel or Escape and never saves a password-confirmed session flag.
Confirmation closes that
event and deletes all its registrations, including unfinished ones. It keeps
earlier/different events intact and allows recreating a corrected event of the
same type and year. Any queued Google removals, including ones from deleted
events, are retried during the next submission. The browser backs up current
attendees first, then attempts queued cleanup. A failure for one record does not
stop other registrations or meal backups. The final result reports confirmed
backups, confirmed removals and failed records separately; failed deletions remain
queued. The admin
submission button remains available when deletions are queued and the list
is empty. Previously published QR links for a deleted event stop working.

Print is available regardless of payment/release. The attendee table has a
checkbox beside each Print button and a select-all checkbox in its Print header.
Select-all applies to currently visible rows; Print selected prints only visible
checked IDs, in the displayed order. A normal row Print click starts a single-ID
job immediately. Both paths prepare an admin/CSRF-protected, ID-only document in
an offscreen iframe, wait for every image, then open the browser printer dialog.
There is one ID per page, always 100 × 148 mm, with zero document margins and no
width input. Up to 100 IDs can be prepared in one job.

The first job prints the actual selected IDs after the admin chooses printer,
paper and quality settings in the normal printer dialog/driver preferences;
there is no separate test-page job. A first-use hint is hidden on that browser
after a successful physical-print confirmation. The app remembers only that
confirmation, not the printer name or driver settings. Those settings remain
managed by the browser/Windows, and the normal print dialog opens for every job.

When the print dialog closes, a popup lists the job's IDs. Unconfirmed IDs start
unchecked. Staff check only physically successful prints and click Mark selected
as printed. Each checked ID uses the existing revision-protected physical-print
confirmation endpoint. Cancelling, closing without selecting, image-load errors
and unchecked/failed IDs do not set printed status. Previously confirmed IDs
keep their status. A stale record reports an error and refreshes its revision;
the admin must review and select it again before retrying confirmation.

**Reprint remaining IDs** saves checked successful IDs first, then starts a new
real print job containing only the unchecked, unconfirmed IDs. It leaves the
current popup open if saving a successful confirmation fails, so no selections
are lost or silently duplicated. The button is disabled once every ID in the
job is confirmed. A cancelled or failed job can be retried without selecting
the same attendees again in the table.

The dedicated print page remains available, with the actual ID, fixed-size Print
ID action and PNG download. Its original manual confirmation checkbox also
remains available. Opening/cancelling the print dialog and downloading never
mark physical printing complete.
The additive `physical_printed` field starts unconfirmed for legacy records,
because the old `printed` field also allowed download confirmations. Existing
data is preserved; admins must confirm physical printing before a new release.
The saved completion mark makes the attendee-list Print button green. The Print
ID button on the print page also turns green immediately after a successful
checkbox save; clearing the checkbox restores the normal button color.
Browser printing uses the user's printer dialog, not unattended direct printing.
The physical size is fixed at 100 × 148 mm. The PNG's DPI metadata requests the
same physical dimensions. Only the ID is included in the print document; use
actual size / 100% in the printer dialog.

Release / Share opens a new browser tab with an ID preview, **Release ID** and
**Share Temporary ID**. The server permits Release ID only after payment and
physical printing are confirmed and staff confirm the physical handover. Only
that action turns Release green. Undo release requires confirmation; payment
or physical printing cannot be cleared while the ID remains released.
Sharing uses the device's file share menu, or downloads the PNG where file sharing
is unavailable, without changing any payment, print or release status. Shared
pictures carry a TEMPORARY ID label until physical printing is confirmed, then
the action becomes Share ID and shares the clean picture. Printable and backed-up
originals never carry the temporary label. The portrait, name and QR stay visible.

The 1000 × 1500 ID uses the uploaded blank background, the existing District 4
logo beside the organization name at the same height and a close, measured gap.
The larger organization heading fills the top, followed by a handwritten event
title using the bundled `Kalam-Regular.ttf` font, and large navy/burgundy theme with gold
outlines, Bible text, portrait, name, direct Area/church values and an unlabelled
QR. A single translucent, softly frosted glass panel spans the logo and
organization through the theme and Bible text, keeping the uploaded background
visible. Text within this panel has no glow. The attendee name retains its
feathered white backing/glow. Each section measures, wraps and
shrinks inside fixed boxes; oversized content is rejected before saving rather
than overlapping the portrait or QR. The title uses General Convention for a
District Convention and Youth Convention for a Youth Convention, with the
event start year. No meal-ticket label, attendee number, birthday, contact,
pastor or church address is printed.

The QR opens a local registration lookup containing only the attendee name,
area, church and event. Opening that URL never changes payment, release or meals.

## Processing tabs and master meal synchronization

Admin has For Processing and Completed Delegates tabs. Paid, Print and Release
must all be green to count as completed; changing one back moves the same delegate
to For Processing. The completed view shows event-wide registration/completion
totals, per-area totals and all tied leading areas based on completed delegates.
Search/area filters operate within the selected tab. View Details fetches private
full registration fields and meal/audit history only for authenticated admins.

Only paid status controls meal eligibility, regardless of Print or Release. The
Android master supplies meal sets, sessions and claims through its separate bearer
API; the first sync imports the app's setup. No website schedule is invented. The
master alone connects to the website; slaves pair/communicate with it over Bluetooth.
The website stores accepted, duplicate and ineligible attempts and immutable audit
records. It transactionally prevents two accepted claims for the same event,
session and ticket. Voids append a reasoned audit and preserve original scan data.
Sync is local-first; Backup Data also exports Event Meals / Event Meal Audit to
Google using verified RAW batch writes. Portraits are never uploaded by this flow.

See [ANDROID_MEAL_API.md](ANDROID_MEAL_API.md) for the implemented version-1 routes,
JSON examples, batch/cursor rules, master-only environment configuration and
Android responsibilities. This backend does not implement Android pairing,
Bluetooth transport, or the scanner application itself. Without a private
EVENT_MEAL_MASTER_TOKEN of at least 32 characters, master synchronization is disabled.
