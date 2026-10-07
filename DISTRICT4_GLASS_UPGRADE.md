# District 4 glass theme

This approved update changes appearance only. It adds frosted menu surfaces, white edge highlights, translucent cards, soft shadows, rounded controls, and the shared blue/lavender palette across the website, including Church Finder and both schedule views. Existing wording, forms, scripts, permissions, map functions, reporting behavior, and color meanings are preserved. The About Developer page does not load the new stylesheet and keeps its previous design.

Only three application files changed in this phase:

```text
templates/base.html          Loads the glass theme except on About Developer
templates/splash.html        Loads the glass theme for the standalone login
static/glass_theme.css       New presentation layer
```

All Python modules and the other templates remain byte-for-byte unchanged. Church Finder, Schedules, progress pages, resource libraries, sermon library, pledges, and the existing reporting/prayer/account forms inherit the styling without changing their embedded templates or application scripts. Map markers, event colors, completion/approval colors, hidden dialogs, and disabled controls keep their existing behavior. The existing calendar still scrolls horizontally on phones.

## Review locally

Restart the website normally, then press **Ctrl+F5**. Check the menu, Church Finder map/search views, schedule controls and dialogs, Pastor's Tool, AO pages, libraries, and login. Screenshots using synthetic data are saved in `.visual_upgrade/glass_v2/screenshots`.

Verification covered 27 rendered pages at 320px, 390px, and 1200px widths. Rendered text, controls and attributes, links, inline styles, and scripts were compared with the immediately previous design. Browser checks exercised menu focus, Church Finder search/view switching, schedule month picker, and existing account/report dialogs. Charts used temporary fixture data and resource API responses were mocked for previewing. The 32 existing cache safety tests passed. These checks used temporary databases and did not call live Google Sheets or query production databases. Native iPhone and Android WebView checks remain manual.

To repeat the isolated comparison:

```powershell
.\.venv\Scripts\python.exe .visual_upgrade/glass_v2/check_glass.py
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_sheet_cache.py
```

## Deploy

Use `deployment/district4_glass_visual_upgrade_20261005.zip`. This complete visual bundle includes the first visual upgrade's dependencies, plus this glass layer, with these nine files:

```text
templates/base.html
templates/splash.html
templates/bulletin.html
templates/ao_tool.html
templates/ao_church_status.html
templates/ui_icons.html
static/visual_upgrade.css
static/glass_theme.css
static/img/thanksgiving-hero.jpg
```

If the first visual upgrade is already installed, only the three application files listed above need updating. Otherwise, upload the full bundle, keeping its folder structure. Keep the existing `static/styles.css`; it remains required and unchanged. Back up the server's current copies, restart/reload the website, then refresh the browser. No Python, database, credentials, new libraries, or migrations belong to this update. Documentation, screenshots, and local verification helpers do not need uploading.

## Roll back this glass update

From the project folder:

```powershell
.\.visual_upgrade\glass_v2\restore_previous_design.ps1
```

Then restart and refresh the website. This restores the previous `base.html` and `splash.html`; the unused new CSS can remain. The script's restore behavior was checked in a temporary copy. The corresponding backup archive is `.visual_upgrade/glass_v2/previous_design.zip`.

To return all the way to the design before the first upgrade, run the command above first, then run `.\.visual_upgrade\restore_original_design.ps1`. Existing original-design backups were retained. For a deployed site, use its own matching pre-upload backup to preserve any server-specific changes.
