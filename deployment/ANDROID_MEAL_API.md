# Android master meal synchronization API, version 1

Implemented in `app.py`. This is the supported website contract, not a description
of Android Bluetooth code. Source snapshots in the integration ZIP are sanitized.
The website changes must be deployed before the production server supports them.

## Setup and ownership

- Base URL: `https://iowod4tool.coresite.pro/event-registration/api/v1`.
- Every endpoint requires `Authorization: Bearer <private master token>`.
- Configure a separate random `EVENT_MEAL_MASTER_TOKEN` of at least 32 characters
  and `EVENT_MEAL_MASTER_ID=master` in the running server's environment. Restart
  the updated app/container. A missing token disables the API with HTTP 503.
- The token is intentionally absent from this document and the ZIP. Provision it
  to the MASTER only, using private app settings, not Kotlin source, a pairing QR,
  a ticket QR, shared admin credentials, or SLAVE devices. Rotating it revokes access.
- There is one authorized master identity per deployment. SLAVEs communicate only
  with that master over Bluetooth. Pairing, transport framing, reconnection,
  permission handling, local atomic claims and offline SQLite belong to Android.
- Website registration/payment data is authoritative. Android master meal-set
  definitions are authoritative for schedules. Importing them never creates
  breakfast/lunch/dinner slots automatically.
- Website event creation remains developer-only in the existing web UI. Android
  selects an existing website event and maps any local app event to its event key.

## Identity and QR compatibility

Website `event_key` and `ticket_id` are 32 lowercase hexadecimal UUID strings.
The event's integer SQLite ID is internal. The per-event attendee number is a
display ordinal and restarts for each event; do not use it as a ticket ID.

Existing printed QR payloads remain:
`https://iowod4tool.coresite.pro/event-registration/ticket/<ticket_id>`.
Parse the exact HTTPS host and path, reject extra path components/query/fragment,
and validate the extracted key against the selected event's downloaded roster.
Opening that URL is public HTML and never claims a meal. The website does not
currently expire an Event ticket automatically; deleting it or its event revokes
it. Archiving an event alone does not revoke tickets.

Master-generated batch, set, session, claim, scanner and audit IDs must contain
only letters, digits, `_`, or `-`, with length 1–96. UUID strings with hyphens work.
Claim and audit IDs must be globally unique across events (use UUIDs). Generate and persist them before sending. Never regenerate them on retry.

## Endpoints

All responses are JSON with `ok`. Browser admin cookies do not authorize these
routes. No CSRF token is needed for bearer-authenticated API calls. Responses are
private/no-store. There is no cross-origin browser/CORS integration requirement
for the native app.

### GET /events

Response:
```json
{"ok":true,"api_version":1,"master_id":"master","events":[{"key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"District Convention 2026","start_date":"2026-12-20","end_date":"2026-12-23","active":1}]}
```
Nondeleted events, including previous events, are returned. Creating a local
Android event does not implicitly create or rename a website event.

### GET /events/{event_key}/delegates
### GET /events/{event_key}/changes?cursor={snapshot}

Both return the same snapshot protocol. The cursor is a content hash, not a date.
```json
{"ok":true,"changed":true,"event_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot":"CONTENT_SHA256","mode":"replace_event_snapshot","delegates":[{"ticket_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","number":10,"name":"Sample Delegate","area":"4","church":"Sample Church","paid":true,"printed":false,"released":false,"processed":false,"meal_eligible":true,"updated_at":"2026-11-07T10:00:00+00:00","revision":2,"ticket_url":"https://iowod4tool.coresite.pro/event-registration/ticket/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]}
```
If the supplied cursor matches, `changed=false` and `delegates=[]` means keep the
existing cached roster. If `changed=true`, atomically replace that event's cached
eligibility roster; missing tickets are revoked. Keep scan/audit history separately.
Persist the new cursor in the same local transaction. A changed empty roster
really clears eligibility for that event. Cursors are scoped to an event. This
avoids timestamp-only deltas missing purged website deletions. There is no paging.
Only complete, nondeleted public registrations are returned. Birthday, contact,
pastor, address and credentials are excluded from the master roster.

`meal_eligible` means **paid**, regardless of Print/Release. `processed` means
Paid AND Print AND Release, used only for the website's two administration tabs.

### POST /events/{event_key}/meal-sets

```json
{"batch_id":"sets-batch-1","sets":[{"set_id":"convention-meals","name":"Convention meal set","revision":1,"sessions":[{"session_id":"opening-dinner","name":"Opening Dinner","date":"2026-12-20","enabled":true}]}]}
```
Response `results`: `[{"set_id":"convention-meals","revision":1,"status":"saved"}]`.
An identical definition can return `unchanged`. Definitions are supplied by the
master on first sync and on later changes. Each set needs a strictly positive
integer revision. A newer revision replaces that set's active session definition;
omitted sessions are disabled, not deleted. Other sets remain unchanged.
Reuse stable session IDs when editing labels/availability. Do not reassign an ID
to another set; use a new session ID when changing the date of a meal that already
has scan history. Same revision with different content and stale revisions are 409.
Sets may have no sessions, and dates follow the app's setup rather than a forced
website schedule. Max 100 sets per batch and 100 sessions per set, within 512 KiB.

### POST /events/{event_key}/claims

```json
{"batch_id":"claims-batch-1","claims":[{"claim_id":"claim-1","ticket_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","session_id":"opening-dinner","scanner_id":"slave-1","served_at":"2026-12-20T18:00:00+08:00"}]}
```
Response:
```json
{"ok":true,"batch_id":"claims-batch-1","received_at":"2026-12-20T10:01:00+00:00","results":[{"claim_id":"claim-1","status":"accepted"}]}
```
Possible per-record statuses: `accepted`, `duplicate`, `unpaid`, `invalid_ticket`,
`invalid_session`; replaying a claim previously corrected can return `voided`.
Syntactically valid unsuccessful attempts are also retained for review. They are
not accepted entitlements. Unknown/disabled sessions, unpaid delegates and tickets
from a different/deleted event cannot produce an accepted claim.

The server uses BEGIN IMMEDIATE and a unique partial index on
`(event_id, meal_key, ticket_key) WHERE status='accepted'`. Distinct claim IDs for
the same entitlement produce one accepted claim and duplicate attempts. Identical
claim IDs/payloads are reused; different content with the same ID returns 409 and
rolls back the entire batch. All dates/times must be ISO 8601 with an explicit
timezone. `served_at` is normalized to UTC; `received_at` is recorded separately.
The server preserves the master's clock timestamp and does not enforce event-day
or time-window attendance. Clock corrections/disputes should be separate audits.

Offline scanning uses the latest cached paid roster. If the website has changed
payment/revocation before sync, the uploaded attempt can be unpaid/invalid; retain
the app's local serving history and show the discrepancy for review. Only one
master should make live venue decisions; website reconciliation cannot stop two
disconnected masters from physically serving the same meal.

### POST /events/{event_key}/audit

```json
{"batch_id":"audit-batch-1","audit":[{"audit_id":"audit-1","claim_id":"claim-1","action":"void_claim","reason":"Operator corrected the scanned ticket","occurred_at":"2026-12-20T18:10:00+08:00"}]}
```
Actions: `note`, `dispute`, `void_claim`. Reason is required, max 500 characters.
Response results: `[{"audit_id":"audit-1","status":"recorded"}]`.
The original scan timestamp, scanner and identity are never erased. Notes and
disputes append history without granting another meal. Void changes the claim's
current status to `voided` and releases its accepted entitlement; a genuinely new
claim then uses a new claim ID. Claim IDs must already exist in the selected event.
There is no arbitrary row overwrite, history deletion, restore action or website
Bluetooth scanner endpoint. More correction actions require a future API version.

### GET /events/{event_key}/sync-status

Returns `ok,event_key,master_id,sets,sessions,claim_counts,last_sync`. Sets contain
`set_id,name,revision,updated_at`; sessions contain
`session_id,set_id,name,date,enabled` (enabled is currently integer 0/1).
`claim_counts` maps current statuses to counts. `last_sync` is the latest batch
receipt timestamp or null. This is a summary, not a complete claim download API.

## Batch retry and error rules

POST bodies: JSON object, 1–100 items in `sets`, `claims` or `audit`; at most
512 KiB. Same event+kind+batch ID and identical canonical JSON returns its saved
acknowledgement, even after a lost response. Keep the original payload unchanged.
Acknowledgements describe that batch at receipt time; they do not undo later
corrections. Reusing a batch ID with different content/master is 409. Per-record
IDs also protect re-batching/retrying records. Each batch is one DB transaction;
schema/identity conflicts roll it back. Item order is part of the batch payload.

Errors: `{ "ok":false,"code":"...","error":"..." }`.
- 400 INVALID_REQUEST: bad shape, ID, timestamp or fields; fix input.
- 401 UNAUTHORIZED: missing/wrong master bearer token; do not retry indefinitely.
- 404 INVALID_REQUEST: deleted/unknown event; select a valid event.
- 409 CONFLICT: reused IDs, conflicting revisions/identities; inspect, do not overwrite.
- 413 TOO_LARGE: split before assigning new batch IDs; no rows were saved.
- 503 NOT_CONFIGURED: server master token missing/too short.
- 503 RETRY: DB operation unavailable/busy; retry the exact batch with backoff.
Unexpected server/network errors also require retrying the unchanged batch, not
creating new claim IDs. Keep a durable Android outbox until acknowledgement.

## Website UI, storage and Google backup

Admin tabs: For Processing and Completed Delegates, calculated from Paid+Print+
Release. Completing/reversing a status moves the existing row without changing
its number. Completed shows event-wide totals, per-area totals and tied leaders.
View Details loads protected full registration information plus meal/audit history.
No full-delegate PII is embedded into public pages or the Android roster.

Meal tables are additive in the same DATA_DIR/app_v2.db:
`event_meal_sets`, `event_meal_sessions`, `event_meal_claims`, `event_meal_audit`,
`event_meal_batches`. Meal claim identity/name/area/church and meal labels are
snapshotted so registration deletion/correction cannot erase serving history.
Deleted parent events with meal data are retained as tombstones and hidden from
the master event list. Existing QR keys are unchanged.

Sync writes locally only; it never uploads portraits or contacts Google. Admin
Backup Data exports IDs/registration rows and then meal/audit records using RAW
batch writes plus readback verification. New worksheets in District4 Data:
- Event Meals: Event | Event Key | Claim ID | Ticket ID | Name | Area | Church |
  Meal Session ID | Meal | Service Date | Served At | Scanner | Master |
  Received At | Status.
- Event Meal Audit: Event Key | Audit ID | Claim ID | Action | Reason |
  Occurred At | Received At | Master | Previous Status.

Local data stays authoritative and retained after backup. Google failure does not
remove the master's accepted sync or the local meal history. The native app must
not call session/CSRF-protected admin export routes or connect remotely to SQLite.

## Deployment and remaining app work

Deploy the updated Flask code and configure the master credential privately.
The current Drive script already supports ID-only backup; no meal script is
required. Android Studio still needs DTOs, Room/SQLite schema, durable outbox,
Bluetooth master/slave protocol, pairing approval/reconnect, scanner UI and actual
device testing. Do not ship a bearer token in this integration ZIP or source code.
