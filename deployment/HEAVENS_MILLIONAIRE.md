# Who Wants to Be Heaven’s Millionaire?

The game lives at `/bible-games` inside the existing Flask website. It is not a second website and does not use Google Sheets for accounts, gameplay, rankings, recovery, portraits or administration.

## Run locally

Use the website's existing `.env` and dependencies, then run from the repository:

```powershell
.\.venv\Scripts\python.exe app.py
```

Open `http://127.0.0.1:5000/bible-games`. If your development server uses another port, use `/bible-games` on that server. Keep `SESSION_COOKIE_SECURE=0` for local HTTP. On the HTTPS VPS deployment set `SESSION_COOKIE_SECURE=1`; the new optional setting applies the Secure flag to the website's existing session cookie. Keep the existing strong `FLASK_SECRET_KEY` stable between deployments.

The game imports and validates all 3,382 questions on startup, ignoring editorial review flags for eligibility. It checks IDs, four distinct options, answer mappings, difficulty ratings and the complete previous 1,600-question pool. Updates are fingerprinted and idempotent. The working question JSON is outside the public static directory; Docker explicitly copies it. Do not expose the project root or this JSON through Nginx. Existing attempts keep their saved question/answer snapshots when the bank is updated.

The bank replaces all 1,035 Scripture section-heading/book-location questions with original questions based on the supplied Thompson **Bible Character Studies** and their supporting Scripture accounts: Noah, Abraham, Jacob, Joseph, Joshua, Gideon, Samuel, David, Solomon, Elijah, Elisha and Daniel. The other 565 questions, including all 100 Master questions, are unchanged. Citations were checked against the supplied EPUB; review flags remain editorial metadata, not an eligibility filter. The original bank backup, local authoring facts and source extracts are under ignored `.cache_optimization_work/heavens_character_studies/`; the EPUB and extracted prose are not shipped. The working JSON records source anchors and its EPUB fingerprint.

The added pool contains 1,782 adapted four-choice questions: 350 from James Stuart Bell's **How Well Do You Know Your Bible?** and 1,432 from Carl S. Shoup's **Test Your Bible Knowledge**. Keys are matched to each supplied EPUB's answer section. Two duplicate printed Shoup numbers are corrected by their surrounding sequence (877 printed as 817; 883 printed as 833); missing punctuation, merged choices and a few OCR errors are repaired. True/false formats, dependent questions, duplicated wording/choices and an excessively long timed item are not forced into the four-choice format. “All of the above” is rewritten so shuffled choices remain valid. Source titles and quiz locations appear after answering. The source EPUBs and preparation extracts remain local and ignored under `.cache_optimization_work/heavens_new_quiz_books/`.

Difficulty ratings use 1 = Beginner, 2 = Intermediate, 3 = Advanced, 4 = Expert, 5 = Master. New questions are rated for familiarity, precision, rarer names, context and comparisons, with editorial adjustments to familiar items. They are estimates rather than ratings calibrated from player results. New counts by level are 234, 583, 646, 278 and 41; the established levels of the previous pool are retained. The administrator question browser displays the rating and its rationale.

The existing local staff/member caches supply identities and the pastoral church directory. If this directory is empty on a new installation, populate it through the website's existing administrator cache-sync workflow before game registration. The game does not trigger that workflow. Directory lookups filter actual pastor records and do not create or modify ministry accounts.

## Accounts, portraits and recovery

New players enter complete name, Area, Church (or the Area Overseer option), a password and their portrait, and confirm the scoreboard notice. The church selector displays the pastor automatically. A generated Player ID and private recovery code are shown once, with a download button. No email address or phone number is collected.

Existing authenticated website users enter automatically. Their account is verified against the local mirror and linked by stable account identity, not a matching name. They add a portrait and confirm the scoreboard notice before playing; their affiliation comes from their account, with no church selector. A game-only login does not grant ministry access. Area Overseers inherit their Area and are shown as **Area Overseer** in place of a church; no individual church or pastor is required. New game-only registrations can also choose **Area Overseer** under their Area. This scoreboard affiliation does not grant administrator privileges.

Photos are decoded and re-encoded server-side as square 320 × 320 JPEGs, at most 60 KiB. EXIF/location metadata and original filenames are discarded. The original upload is not retained. The server accepts still JPEG/PNG/WebP files, limits uploads, and rejects unsafe or oversized image dimensions. A profile update replaces the previous portrait. Photos stay outside `static/`, Git and Docker images; the portrait endpoint requires a signed-in player or authorized game administrator. The scoreboard shows faces and names to signed-in players, as explained during registration. No camera runs during gameplay.

At the maximum portrait size, 10,000 players use approximately 586 MiB of image storage. The game stores one current portrait per player. Database/history size grows separately.

Forgotten passwords use the saved Player ID and recovery code. Recovery rotates the code and invalidates existing game credentials/sessions without deleting history. If the code is also lost, the player can submit an administrator-help request. The administrator must independently verify the person, record how that was done, and privately give them a one-use reset code valid for 15 minutes. Knowing Area/Church/Pastor alone is insufficient.

## Gameplay and interface

- The exact title is **Who Wants to Be Heaven’s Millionaire?**
- Mobile portrait, iPad portrait, and iPad landscape layouts use touch-sized controls. Landscape puts the host beside the question with the ladder alongside; portrait keeps the ladder visible on iPad and offers a ladder drawer on phones. Longer questions can scroll vertically. Device rotation does not restart the timer.
- After registration and recovery-code handoff, a loading bar prepares the initial artwork and first music track. It reports files processed, includes retry handling, and rotates honesty reminders, original Bible-verse paraphrases and the play-for-fun notice. Returning players also prepare files; an already-active game resumes immediately so loading does not freeze its deadline.
- Result navigation (Next question or Back to the studio) is above the artwork and explanation, including milestones and final victory.
- State checks and lifeline effects update question controls in place, retaining the host/studio DOM and scroll position. Public artwork uses the existing ten-day HTTP image cache plus a bounded 24-image decoded-memory cache. Current/next stage backgrounds and possible result artwork preload during the current question. Preloading does not fetch future question content or start the next round's timer. Source failures resolve without blocking game controls.
- Both supplied MP3s are included unchanged. Track 1 accompanies early stages; track 2 begins at Question 11. Music starts with a player gesture and has a persistent mute preference. Browser audio policies can require a tap on the music button. Later assets load when needed; the app does not preload the entire 81-image collection.
- Questions have 15 slots across five levels: Beginner 1–5 (30 seconds), Intermediate 6–7 (45), Advanced 8–10 (45), Expert 11–13 (60), Master 14–15 (60). New games randomly draw within each level while excluding the previous question's category, including at level boundaries. A category can return after an intervening category; there is no fixed category sequence. Change Question excludes the categories of the previous, current and following saved questions, so a replacement preserves variety on both sides and retains its level. Choices shuffle server-side. Used question IDs cannot recur; character-study questions from the same Scripture chapter also cannot recur across levels or through Change Question. Existing attempts retain their saved sequence when the code/bank is updated.
- Each new 15-slot game contains exactly six added quiz-book questions (40%) and nine previous-pool questions (60%). Their positions vary within the allowed levels. The two added books form a combined pool; no separate split between the authors is forced. Change Question stays in the replaced slot's source pool, preserving the ratio. A game that ends early may not display all 15 slots. Existing active attempts retain their already-saved source mix.
- Questions actually shown to each player are permanently excluded from their later games. This includes a question discarded through Change Question. Unreached preselected slots remain available because the player has not seen them. Stable IDs, normalized wording, and matching passage/answer fact keys catch identified duplicates and paired character-study variants across sources. This history survives restarts, recovery and bank updates; it is separate for each player. The migration backfills shown questions and replaced originals from existing attempts. Planning and history writes are transactional; duplicate starts resume the same attempt. A finite pool can run out: the server refuses a new round when a fresh 40/60 game cannot be arranged and never silently clears history or recycles questions.
- Thompson, Shelley, Wiersbe, NIV attribution labels and other supplied resource-book references are removed from player question wording. Full source titles, references and PDF pages appear inside the explanation after the round is resolved; new character questions also name the character-study profile there. Biblical book names that are intrinsic to a question/choice remain. Bible Hint gives complete local passage text and adjacent context for reference-identification questions, without book/chapter/verse labels that would reveal the answer. If the validated passage is unavailable, the hint remains unused and no time extension is granted. Other Scripture questions retain source-reference clues; historical hints give source chapter/page locations without revealing resource titles.
- Scores use the agreed ladder, with guarantees at Question 5 (1,000) and Question 10 (32,000), and 1,000,000 for Question 15. Wrong answers/timeouts retain the most recent guarantee; an ordinary walk-away retains earned points. Every attempt, including zero, remains recorded.
- Answers submitted by the current interface include the exact visible option text. The server resolves it against the round’s saved shuffled choices, preserving the selected answer after a hint or refresh. The round token, excluded choices and server-held correct answer remain authoritative. Legacy index-only clients continue to work.
- Lifelines are once per game. The server freezes the remaining question time when a lifeline starts. Bible Hint and Audience stay paused through reading until Continue game (or Escape); 50:50 and Change Question resume after their two-second animation. Refresh/reconnect restores the active lifeline and its saved remaining time. Resume is idempotent, and a stale resume cannot close a newer lifeline. Duplicate lifelines cannot reset time; an already-expired question cannot be rescued. Change Question preserves the time remaining. Simulated audience percentages are identified as simulated and can be wrong.
- Default sweetened offers are enabled at Questions 6, 11 and 14, when earned points are at least 1,000. The default fixed bonus is 1,000, capped at a total offer of 1,000,000. These are initial configurable settings, not a universal percentage rule. Administrators can change amounts/eligibility or disable offers. Each attempt keeps a frozen copy of its settings. Accepting ends the game with the recorded offered total; rejecting is logged. Offer and answer-confirmation dialogs do not pause the clock.
- Server deadlines survive reload and disconnects. Late submissions cannot restore time or forge a score. Outside an active lifeline, a network disconnect does not pause the question; the UI explains this before play. An expired attempt is finalized on its next game/scoreboard/admin request, even if the player closed the browser.
- Visibility/focus events are debounced. The first interruption warns; the second ends the attempt as disqualified, excluded from official rankings. Rotation is not an interruption. Calls, screen lock, app switching and navigation can trigger the rule, which is explained before play. This is a browser deterrent, not proof of cheating or device monitoring. Missed/manipulated browser events cannot establish what occurred on a device.

## Administration and rankings

The locally verified District Overseer account and the existing locally verified developer/owner account can access **Game administration** from the game menu. Other users cannot configure the game, export reports, inspect other players' private histories, approve recovery or read the full question bank.

The desk includes totals, games by date, scores by Area/church, player search/history, interruption records, imported question review, offer settings, recovery approvals, CSV export and administrative audits. CSV text is escaped against spreadsheet formula injection. It does not export passwords, recovery codes or portraits.

**Top Scores** uses each player's best eligible finished attempt: highest score first, then fastest total completion time. For equal personal best scores, the player’s fastest attempt is selected. Completion time uses existing server start/finish timestamps, including result-reading, pauses and disconnected time, rounded to milliseconds. It is displayed as minutes:seconds.milliseconds; older results are ranked using their saved timestamps. Missing or invalid durations sort behind recorded durations at the same score. Equal score and duration share a rank; finish date/ID provide stable display ordering without changing that rank. **All Game Results** includes zero-point, wrong-answer, timeout, walk-away, sweetened, winning and disqualified results. **My Game History** shows the player's own complete history. Filters use Area, Church and Today/This Week/This Month/All Time; weeks begin Monday and dates use Asia/Manila. Disqualified attempts remain in history but do not contribute to highest/cumulative eligible score. Church/pastor snapshots remain attached to attempts; profiles can show the directory's current pastor.

## Persistence, deployment and backups

Game-owned persistent files are:

```text
DATA_DIR/
  heavens_millionaire.db
  heavens_millionaire.db-wal    # when WAL has uncheckpointed commits
  heavens_millionaire.db-shm   # SQLite coordination file while open
  heavens_millionaire_photos/
    <random-id>.jpg
```

The existing Compose volume mounts DATA_DIR at `/data`, so accounts, scores, question history and portraits survive container replacement. The existing systemd deployment keeps the same database location across Git pulls; ignored runtime databases are not part of the code commit. Do not put portrait storage under an ephemeral container directory or add it to a public Nginx alias. Keep the current single Gunicorn worker/four-thread deployment because other website modules rely on process-local job state. SQLite WAL, foreign keys, busy timeouts, short write transactions and a unique active-attempt index protect game operations. No per-second server timer/polling worker is needed.

The updated Dockerfile includes the question bank and existing static-copy rule includes optimized images, JavaScript/CSS and both music tracks. No new Python dependency is required; Pillow was already pinned. The repository's existing main-branch GitHub Actions workflow pulls code and restarts the systemd website automatically. Do not include the locally modified tracked ministry databases in a game commit: the workflow deliberately rejects database-file changes. Preserve existing environment secrets and game storage. Check that the reverse proxy allows at least a 6 MB request body for portrait uploads, and verify expected simultaneous-player capacity in staging; asset size alone does not establish server capacity.

For a simple consistent backup, stop the application using the existing deployment workflow and copy both the entire game database set and its portrait directory. When copying live database contents, use SQLite's backup API for the database and coordinate portrait writes; copying only a live `.db` can omit committed WAL data. The existing stopped-container whole-`/data` backup procedure already covers both. Restore accounts/history and matching portraits together. The private packaged question bank is re-imported idempotently; active question/choice/settings snapshots are stored separately in game records.

## Pause persistence and playback checks

The game database gains the nullable `game_answers.pause_started_at` column at
startup. Existing questions, shuffled options, choices and deadlines remain
intact. A paused question is excluded from timeout sweeps; resume shifts the
saved deadline by the actual pause duration and clears the pause atomically.
The total start-to-finish leaderboard duration still includes lifeline reading.

Server checks cover all lifelines, restart/refresh, duplicate and stale resumes,
late requests, interruption rules and upgrading an existing game database.
The focused real-browser flow covers phone and tablet layouts, a long paused
hint restored after reload, Escape to continue, all four lifelines, unchanged
host/studio nodes across state updates and Change Question, preloaded stage
artwork reused without another download, next-round timers starting only on
Next, and top navigation at ordinary, milestone and million-point results.

## Hint and completion-time checks

The hint regression checks cover all four shuffled correct-answer positions,
refresh, a shifted numeric slot, wrong/unknown/eliminated selections, unavailable
local storage, and chapter boundaries. Bible context is read only from the
validated local Bible Study database; game requests do not call Google or a model.
Leaderboard checks cover tied million-point wins, fastest personal bests,
score priority, identical-duration ties, invalid old timestamps and filters.
The focused Chromium test runs the real hint/answer and leaderboard flow on
phone and desktop sizes. Run it with `HEAVENS_BROWSER_TESTS=1`; an optional
`HEAVENS_CHROMIUM_PATH` can select an installed Chromium executable.

## Validation

The current run passed all **49 server/integration checks** (53 discovered, with four optional browser checks skipped). The focused real-browser hint/answer/completion-time and pause/preloading/navigation checks also passed separately in Chromium on phone, tablet and desktop sizes. The two earlier broader Chromium/Safari-WebKit checks remain available through the optional browser suite. They cover automatic website-account entry, photo-only onboarding for pastors and AOs, photo preview without church selectors, account-derived affiliation, and starting a game. They also verify that game-only Area Overseer labels grant no administrator access, the exact six/nine source ratio, cross-game history and cross-source fact exclusions, migration/recovery/restart behavior, exhaustion without recycling, different categories across level boundaries, Change Question respecting both neighbors and its source pool, and the unchanged previous questions.

Focused server/integration checks:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_heavens_millionaire.py -v
```

Include browser checks when Playwright and its Chromium/WebKit browsers are installed:

```powershell
$env:HEAVENS_BROWSER_TESTS='1'
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_heavens_millionaire.py -v
```

These use temporary synthetic accounts, portraits and church data. Tests cover all stages, random selection from the eligible level pool, different categories on consecutive questions, replacement at first/last questions and level boundaries, character-study family exclusions, heading-question replacement and unchanged Master/other questions, 15-question victory, guarantees, wrong/zero scores, authoritative deadlines, refresh, private answers, once-only lifelines, persistent pauses and idempotent resume, replacement, offers and frozen rules, interruption debounce/disqualification, duplicate concurrent answers, forged scores, auth/CSRF, website identity/photo gating, portrait compression/privacy, recovery rotation/admin verification, filters/CSV, import validation/idempotence, preserving active questions during bank updates, and real-website Google-sync bypass.

Browser checks exercise registration through the real UI, loading progress, photos on scoreboards, lifelines, correct answers, saved walk-away results, iPad rotation/reload in Chromium and Safari/WebKit, no horizontal overflow, touch target sizes, and loading both supplied music tracks. Screenshots are saved under `.cache_optimization_work/heavens_millionaire/`. These are browser/device emulations, not a physical iPad camera test.

The broader runtime/cache/activity check ran 208 tests: 201 passed, four failed and three errored. All seven event-registration failures were independently reproduced at unchanged HEAD `36d82ad` in an isolated worktree. They concern existing event HTML expectations, deletion-migration indexes, scanner size handling, Drive cleanup wording and ID-release confirmation; game work does not change those flows. Their baseline log is `.cache_optimization_work/heavens-baseline-tests.log`.
