# Harvest Convention ID template

`harvest-convention-2026-template.png` is the finished 1024 × 1536 blank template.
The event title and theme retain the prominence of the supplied reference.
The app inserts only the attendee photo, name, area, church name and unique ticket QR.

## Use in the app

Open Event Registration → Admin → Create Event, or edit an existing event under
Event & backup → Edit event / ID design.

1. Select the event and dates.
2. Upload the finished design using **ID template picture**. This input is always visible.
3. Fill in **Theme** and **Text — Bible verse / reference**, as before.
4. Click **Preview ID**, then **Create Event** or **Save event**.

There is no design selector. The theme and Bible text remain editable event
information for the registration page; the app never redraws them on the uploaded
ID template. The logo, organization, event title and other fixed artwork must
already be part of the picture. Only the attendee photo, name, area, church and
unique ticket QR are added.

Use portrait 2:3 artwork with the same five blank field positions as this sample.
Uploading artwork with unrelated field positions requires a corresponding renderer
change. Choosing a file immediately shows the uploaded picture beside the form;
Preview ID shows that picture with sample attendee details.

When editing an event that already uses a finished template, leave the file input
empty to keep it. A newly uploaded picture replaces it. An event using the original
layout must upload a finished template when saving through this editor. Its older
layout remains available for existing records until then.

In the ready-made layout, square camera captures retain their bottom edge so lower
faces and shoulders are not cropped off.

Missing artwork does not prevent saving attendee details or completing camera
capture. Upload the missing artwork in the event editor before printing.
The bundled Harvest PNG also supplies a fallback
if its saved copy in `event_assets` is missing.

The renderer exports 1000 × 1500 PNGs for the fixed 100 × 148 mm print layout.
The print document and PNG DPI metadata both specify that physical size; no
editable width control is shown. Bulk print uses one ID per page and asks staff
which physical prints succeeded before setting their printed status.
Ready-made field coordinates in that output:

| Field | Bounding box (left, top, right, bottom) |
| --- | --- |
| Photo interior | 329, 624, 671, 921 |
| Name | 130, 940, 870, 1018 |
| Area | 380, 1025, 620, 1067 |
| Church | 125, 1069, 875, 1121 |
| QR placement region | 338, 1140, 662, 1464 |

The QR fills a 324-pixel square with a four-module white margin. Nearest-neighbor
scaling preserves sharp black and white edges. A temporary-ID banner remains to
the left of the portrait, away from the attendee fields and QR.

## Image generation provenance

Created with the built-in `image_gen` tool in edit mode, with an opaque background.
Reference/edit target: `Red Harvest Convention Badge with Anime Portrait.png`.
The generated PNG was copied into this project unchanged.

Final prompt:

> Use case: precise-object-edit. Asset type: production-ready reusable event registration ID template, portrait 2:3 aspect, 1024x1536 or higher. Edit target: the supplied Red Harvest Convention Badge image. Preserve its composition extremely closely: same red textured header, church crest and exact organization lettering, silver/red sweeping ribbons, GENERAL CONVENTION 2026 banner, THEME label, giant italic theme reading “The Urgent Call For The Great Harvest”, Matt. 9:35–38 scripture, wheat ornament, sunset sky, mountain and harvest field with workers. The event title and theme must remain just as large and prominent, occupying the same upper 41% of the image. Do not redesign or enlarge attendee area. Make ONLY these removals to create a reusable template: remove the anime portrait inside its central rounded rectangular frame (x about 33%-67%, y41.5%-61.8%), replacing the entire portrait interior with clean solid deep burgundy; retain its white/red frame. Remove the sample name Ptr. Pijeme F. Walawala from the wide red/silver banner at y62%-68%, leaving a smooth empty red banner with the silver frame unchanged. Remove AREA 7 lettering from the small silver plaque at y68%-71%, keeping plaque blank. Remove Miramonte Church lettering at y72%-74%, leaving clean dark red background there. Remove every black QR module from the large bottom QR panel at x31%-69%, y75%-98%, leaving the inner panel completely plain white, preserving the red/white rounded frame. No placeholder words, no dummy text, no silhouette, no QR pattern anywhere. All other words and artwork must remain. Render the flat finished template edge to edge, no mockup, no desk, no perspective. Sharp typography and rich print-ready detail.
