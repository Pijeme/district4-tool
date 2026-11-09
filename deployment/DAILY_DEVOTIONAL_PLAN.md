# Daily verse and leadership devotional

`daily_devotional.py` owns the reading schedule and prepared reflections. The
old `VERSE_REFERENCES` list has been removed from `app.py`.

The repeating calendar starts with Year 1 in 2026 and Year 2 in 2027. Each year
has 52 weeks of seven daily readings and a separate December 31 reflection.
Year 1 addresses personal character and service; Year 2 revisits the passages
to develop people and healthy ministries. Each day includes a reflection,
question, practical action and prayer. February 29 has a separate entry and
does not move the March readings. There are 730 ordinary entries plus the
leap-day entry.

The short daily NIV passage comes from the validated Bible Study collection.
The shortcut on its card opens Bible Study at that passage. Existing Bible
Study login and access rules still apply.

When available, the existing source loader adds the matching article from the
user's indexed NIV Maxwell Leadership Bible, third edition. It retains the
source text and identifies the daily exercises separately. Prepared original
companion reflections appear immediately, including when the book, Bible
storage or background source loader is unavailable. These reflections are not
represented as John Maxwell's writing or an official Maxwell reading plan.

Publication adds a `plan_key` column to `daily_devotion_posts`. Today's old
rotation is updated in place, preserving its existing comments and reactions.
Archived days remain unchanged. A source note that finishes loading can enrich
today's reflection; subsequent source outages do not remove a published note.
No AI request or new package dependency is required.

## Sermon Notes selection controls

The website and standalone Android app share the Bible reader's selection
palette and Copy icon. Selecting sermon text temporarily replaces the normal
Create/All Notes/Read/Save/Delete controls in the sticky top dock. The compact
selection bar is about 60 CSS pixels tall, with extra font/colour commands
under More formatting. Dismissal, selection collapse or leaving Notes restores
the normal controls. Sermon formatting still uses
the existing editor range, `notesFormat`, native clipboard fallback and save
queue. Existing font, colour, undo and clear-format controls remain available.
The Android copies live in the neighbouring `Stand Alone Bible Study App`
project under `app/src/main/assets/runtime`.
