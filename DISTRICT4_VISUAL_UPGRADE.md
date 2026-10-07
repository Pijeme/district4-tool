# District 4 visual upgrade

Implemented locally on October 5, 2026. This update changes presentation only:

- Navy header and frosted sidebar with authenticated greeting, SVG icons, active link, keyboard focus, and reduced-motion support.
- Single-column Bulletin Board with the existing verse over a landscape image, church initials, category badges, and readable prayer details.
- Six AO utility cards, preserving their existing colors and actions. Developer cache controls remain restricted by the existing template condition.
- Church Status month controls, year selector, summary layout, church cards, report dialogs, and print options.

The mockup's additional tools, search/filter behavior, notifications, likes, bottom navigation, and invented reporting states were excluded. The existing forms, URLs, role visibility, approval logic, report generation, cache behavior, Google Sheets integration, and assistant scripts remain intact. Only the sidebar presentation script changed.

## Preview and verification

Restart your local website as usual, refresh with **Ctrl+F5**, and visit Bulletin Board, AO Tool, and Church Status. Test the menu, month expansion, prayer details, account and announcement dialogs, and print options. Check a phone as well as a desktop. Opening a dialog is safe for inspection; submitting its form still performs the existing real action.

Screenshots created from synthetic test data are in [.visual_upgrade/screenshots](.visual_upgrade/screenshots). These do not contain live church data. Chrome checks covered 320px, 390px, and 1200px widths, horizontal overflow, existing dialogs, menu focus and active links, and reduced motion. Native Android WebView and iPhone device testing remains a manual check.

Verification completed:

- 32 existing cache safety tests passed against temporary databases and mocked Google Sheets.
- Original and upgraded rendered forms, input fields, links, inline handlers, and application scripts matched across developer AO, ordinary AO, Pastor, DO, and Sub AO cases. Guest and Member menu visibility also matched.
- Root Python files and the live `app_v2.db` retained their pre-upgrade SHA-256 fingerprints. No live Sheets calls occurred during these checks.

To repeat the isolated template check:

```powershell
.\.venv\Scripts\python.exe .visual_upgrade/check_visuals.py
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_sheet_cache.py
```

## Update the main website

The visual-only ZIP is [deployment/district4_visual_upgrade_20261005.zip](deployment/district4_visual_upgrade_20261005.zip). It contains these seven files, preserving their folders:

```text
templates/base.html
templates/bulletin.html
templates/ao_tool.html
templates/ao_church_status.html
templates/ui_icons.html
static/visual_upgrade.css
static/img/thanksgiving-hero.jpg
```

The hero image is reused without modification and included so the background is available on the server. Install all files together, then restart/reload the website and refresh the browser. The original `static/styles.css` is still required and was not changed. No Python files, databases, credentials, libraries, or migrations are part of this visual update.

Back up the website's current copies before uploading. The included original-design backup reflects this local project's state before the redesign; server-specific changes need the server's own backup. The documentation and `.visual_upgrade` helpers are local review/rollback aids and do not need uploading. The earlier cache deployment ZIP remains separate and unchanged.

## Return to the original design

From this project folder, run:

```powershell
.\.visual_upgrade\restore_original_design.ps1
```

Then restart the website and refresh with **Ctrl+F5**. The script restores the four original templates and the original stylesheet from `.visual_upgrade/originals`. New visual assets may remain in the folder; the restored templates no longer use them. The script leaves backend files and databases untouched. Its restore behavior was checked in a temporary copy.

Alternatively, [original_design_20261005.zip](.visual_upgrade/original_design_20261005.zip) contains the original files with their folders. For a deployed website, restore its own pre-upload backup or these matching originals and reload it. To reapply the upgrade later, extract the visual update ZIP again.
