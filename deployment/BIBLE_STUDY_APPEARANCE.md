# Bible Study appearance and editor update

Prepared 9 November 2026. Website changes are local source changes; the live
server has not been deployed by this task.

The Bible Study page uses the Android app's wood illustrations, local Lora,
Space Grotesk, Bitter and Rye typography, and warm paper/teal/brown palette.
The website's main banner and burger menu remain in the existing base template.
The reader also includes loading notices and the complete commentary section
list, counts and Previous/Next navigation.

The website artwork is a 180,728-byte WebP encoded from the app's original
2,891,811-byte PNG. Five WOFF fonts total 501,632 bytes. These files are hosted
under `static/bible-study`; font licenses/source notes are included. The combined
new artwork/fonts total 682,360 bytes. The page uses local fonts.
Public WOFF fonts receive the existing ten-day static-image cache policy;
private files retain their own cache restrictions. Versioned stylesheet/script
URLs refresh the changed interface when the website is deployed.

Both the website and Android editor now provide:

- Read Notes, Save and Delete in the top actions, with deletion confirmation.
- Formatting that appears for selected editor text, preserving the selected
  range while using buttons, fonts, sizes and colors. Outside taps/Escape close it.
- Editable Theme and Text/reference lines with pencil icons; the old Editing
  tools and Edit Text & Theme launch buttons are removed. Sync status stays plain.

For iOS, the reader tracks both visual-viewport resizing and panning while the
keyboard is open, and resets its offset after dismissal. The compensation is
limited to iOS; Android retains its existing positioning. This follows the
visual-viewport behavior described in WebKit issues
[311821](https://bugs.webkit.org/show_bug.cgi?id=311821) and
[292603](https://bugs.webkit.org/show_bug.cgi?id=292603).

Validation: all 431 website tests pass, including WebKit tests with simulated
keyboard height/offset changes, dismissal and an Android control case.
The Android project passes 20 checks with Chromium/WebKit editor scenarios,
compilation/lint, debug assembly, signature verification and 16 KB ZIP alignment.
Website desktop/phone previews verify local fonts/artwork load, no missing assets
or JavaScript errors, and visible main banner/menu. Physical iPhone keyboard
verification is still required; browser simulation cannot reproduce its OS keyboard.

Deployment uses the normal website update process. No environment-variable or
database-schema changes are needed. No production server was restarted and no
running backup was interrupted for this work.
