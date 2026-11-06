# Event Registration

The shared public link is `/event-registration`. Its Registration tab uses the
one active event. The Admin tab uses the server-side password `EventAdminD4`
and a four-hour session. Only the signed-in developer account, verified against
the local Accounts cache, can create or edit an event. Event admin access alone
does not grant that permission.

## Events and registration

The developer's Create Event dialog offers District Convention and Youth
Convention, start/end dates, a theme, Bible text/reference and a blank ID
background upload. The start year forms the stored Event value, for example
`District Convention 2026`. Preview ID renders the actual layout with sample
attendee details. Saving checks that the text fits at readable font sizes.
Creating a new event makes it active and preserves all earlier events and
attendees. The admin event dropdown can view earlier collections. An event
with registrations must keep its type and year; dates, theme, text and
background can still be edited. Changes to the design apply to future prints.

The public form collects Name, Bday, optional Contact #, Area, Church Name,
Church Address and Pastor. Area and church selections come from cached
Accounts data. The server fills address/pastor; New church permits manual
entry without adding an Accounts row. Details are saved locally before the
separate camera page. Registration is completed with a live-camera capture;
there is no photo picker. Cameras require HTTPS or localhost. Camera switching,
capture, preview and retake are supported. Retries reuse the same registration.
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

## Local storage and Google submission

`registration_events`, `event_attendees` and `event_request_limits` are additive
tables in the existing app database. Pictures are immutable PNGs under
`DATA_DIR/event_assets`. Keep that directory together with the database during
server moves and backups. Local data and images remain after successful cloud
submission. No cloud import replaces local records.

After capture, the confirmation page attempts to back up the portrait to the
same Drive folder used by CCM. A failed upload keeps registration successful
locally and can be retried on that page or by Admin. Filenames are exactly
`Event>Full Name>10.png` for the tenth attendee; unsafe filename separators are
replaced with underscores. Event/record/content identity distinguishes the
same filename in different events and reuses confirmed retries.

**Update the existing CCM Apps Script deployment** with the complete updated
`deployment/ccm_drive.gs`, then publish a new version of the existing deployment.
Keep its URL and API token. The extension preserves CCM uploads and adds event
filenames and identity descriptions. An older deployment is detected; local
event pictures remain available until the script is updated. Direct Shared
Drive uploads also support the event filenames without this bridge.

Submit to Google Sheets processes the selected event one attendee at a time.
It uploads/verifies portraits and writes the worksheet named `Event Registration`
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
start red and turn green on a click. Changing back requires Yes, change status
or Cancel in a dialog. Revision checks prevent stale windows from overwriting
newer local changes. Admin mutations require CSRF protection.

Delete registration requires confirmation and a current revision. It hides
the attendee and revokes their QR, photo and print access immediately; existing
attendee numbers never change or get reused. A queued deletion is retained
locally until the next Submit to Google Sheets removes and verifies the
corresponding remote row. Only then is that local registration purged. Deleted
registrations are never uploaded again. Private picture files remain as upload
history; deletion of a registration does not trash the shared Drive folder.

Only the developer can see or invoke Delete event. Confirmation closes that
event and deletes all its registrations, including unfinished ones. It keeps
earlier/different events intact and allows recreating a corrected event of the
same type and year. Any queued Google removals, including ones from deleted
events, are processed before registrations in the next submission. The admin
submission button remains available when deletions are queued and the list
is empty. Previously published QR links for a deleted event stop working.

Print is available regardless of payment/release. It opens a dedicated page
with the actual ID, printer-dialog and PNG-download actions. A separate
checkbox records successful printing or downloading; opening/cancelling the
printer dialog and downloading alone never mark completion automatically.
Browser printing uses the user's printer dialog, not unattended direct printing.
The default physical size is 70 × 105 mm, adjustable by width at a fixed ratio.
Only the ID is included in the print document.

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
area, church and event. Actual meal redemption is reserved for the later meal
workflow; scanning currently performs no payment, release or meal-use mutation.
