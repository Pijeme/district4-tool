"""Shared login history schema for authentication and area notifications."""

import sqlite3


LOGIN_EVENT_COLUMNS = (
    "created_at", "username", "full_name", "role", "church_id",
    "church_address", "area_number", "sub_area", "ip_address", "user_agent",
)
LEGACY_COLUMNS = {"created_at": "logged_in_at", "full_name": "name"}


def _columns(db):
    return {row[1] for row in db.execute("PRAGMA table_info(user_login_events)")}


def ensure_login_event_schema(db):
    """Upgrade either historical schema in place, preserving IDs and events.

    The caller owns the transaction. If a partial upgrade left both names,
    keep the legacy columns and copy any missing canonical values.
    """
    db.execute("""
        CREATE TABLE IF NOT EXISTS user_login_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            username TEXT NOT NULL,
            full_name TEXT,
            role TEXT,
            church_id TEXT,
            church_address TEXT,
            area_number TEXT,
            sub_area TEXT,
            ip_address TEXT,
            user_agent TEXT
        )
    """)
    columns = _columns(db)
    for column in LOGIN_EVENT_COLUMNS:
        legacy = LEGACY_COLUMNS.get(column)
        if column not in columns:
            try:
                if legacy in columns:
                    db.execute(f'ALTER TABLE user_login_events RENAME COLUMN "{legacy}" TO "{column}"')
                else:
                    db.execute(f'ALTER TABLE user_login_events ADD COLUMN "{column}" TEXT')
            except sqlite3.OperationalError:
                # Another worker may have completed this migration.
                if column not in _columns(db):
                    raise
            columns = _columns(db)
        if legacy in columns:
            db.execute(f'''UPDATE user_login_events SET "{column}" = "{legacy}"
                WHERE ("{column}" IS NULL OR "{column}" = '') AND "{legacy}" IS NOT NULL''')
    db.execute("CREATE INDEX IF NOT EXISTS idx_user_login_events_created ON user_login_events(created_at)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_user_login_events_scope ON user_login_events(area_number, sub_area, role)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_user_login_events_username ON user_login_events(username)")


def insert_login_event(db, values):
    """Populate legacy aliases too if they remain with NOT NULL constraints."""
    values = dict(values)
    columns = _columns(db)
    for canonical, legacy in LEGACY_COLUMNS.items():
        if legacy in columns:
            values[legacy] = values[canonical]
    names = ', '.join(f'"{column}"' for column in values)
    placeholders = ', '.join('?' for _ in values)
    db.execute(f'INSERT INTO user_login_events ({names}) VALUES ({placeholders})',
               tuple(values.values()))
