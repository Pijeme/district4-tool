# Bible Study startup and bulletin update

Prepared 9 November 2026.

The bulletin places an image link beside the NIV reference. Its generated WebP
is `static/bible-study/open-in-bible-study-v1.webp` (10,964 bytes), displayed at
190 x 38 pixels with high-contrast navy/ivory lettering and an accessible label.

The Android loading screen remains visible while the backend, current Bible
passage, private sermon-note list and current-chapter study tools prepare. A
progress bar reports completed stages. Thirty local NIV study verses rotate
while waiting. The reader opens after the Bible and notes are ready. Optional
study tools retain their availability messages; core preparation failures offer
Retry. No Drive sync starts and existing notes are not overwritten.

Only current-passage data and catalogues are warmed; the multi-gigabyte library
stays on disk. A bounded, owner- and content-specific metadata cache avoids
revalidating unchanged sermons when reopening a list. Full note reads and all
writes still validate content. A desktop check of 514 backed-up notes measured
about 1.0 seconds cold and 0.06 seconds warm; phone timings are separate.

Both readers keep the primary underline/highlight palette. More formatting has
a half-width font-family control beside size/grow/shrink, followed by a second
row with case, Bold, Italic, text colour, Clear and Undo. Duplicate underline/highlight controls were removed.

## Android source

The standalone Android source and its prepared source package are kept locally,
per the user's publishing preference. They are excluded from this repository
and Docker image. The GitHub repository publishes the website changes only.

## Generated asset record

Built-in imagegen was used. Prompt: a compact, wide Bible Study button with an
open Bible at the left, warm ivory parchment, an antique-gold border and large
navy text exactly "Open in Bible Study"; clean subtle texture, no extra words
or watermark. The generated raster was resized and compressed as WebP; display
framing removes unused margins.

The Android app reuses the website About Developer support card after startup
is ready, at most once per 72 hours, including warm resumes. Its cadence is
stored in private app settings across upgrades. The website reminder behaviour
is unchanged. A first-open guided tutorial is under discussion and has not
been implemented.
