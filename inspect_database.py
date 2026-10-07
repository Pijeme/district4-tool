import sqlite3
import os
from datetime import datetime

# ============================================================
# District 4 Tool - SQLite Database Inspector
# READ-ONLY DIAGNOSTIC SCRIPT
# ============================================================

DB_FILE = "app_v2.db"
OUTPUT_FILE = "database_report.txt"


def format_bytes(size):
    """Convert bytes into a readable size."""
    size = float(size)

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:,.2f} {unit}"
        size /= 1024

    return f"{size:,.2f} PB"


def safe_identifier(name):
    """Safely quote an SQLite identifier."""
    return '"' + name.replace('"', '""') + '"'


def main():

    print("=" * 70)
    print("DISTRICT 4 TOOL - DATABASE INSPECTOR")
    print("=" * 70)

    if not os.path.exists(DB_FILE):
        print()
        print(f"ERROR: {DB_FILE} was not found.")
        print()
        print("Put inspect_database.py in the SAME folder as app_v2.db")
        print("and run it again.")
        return

    db_size = os.path.getsize(DB_FILE)

    print()
    print(f"Database: {DB_FILE}")
    print(f"File size: {format_bytes(db_size)}")
    print()
    print("Opening database in READ-ONLY mode...")

    # --------------------------------------------------------
    # IMPORTANT:
    # mode=ro prevents this diagnostic script from modifying
    # the database.
    # --------------------------------------------------------

    absolute_path = os.path.abspath(DB_FILE).replace("\\", "/")

    conn = sqlite3.connect(
        f"file:{absolute_path}?mode=ro",
        uri=True
    )

    cursor = conn.cursor()

    report = []

    report.append("=" * 70)
    report.append("DISTRICT 4 TOOL - SQLITE DATABASE REPORT")
    report.append("=" * 70)
    report.append("")
    report.append(f"Generated: {datetime.now()}")
    report.append(f"Database: {os.path.abspath(DB_FILE)}")
    report.append(f"Database file size: {format_bytes(db_size)}")
    report.append("")

    # ========================================================
    # DATABASE INFORMATION
    # ========================================================

    print("Reading database information...")

    page_size = cursor.execute(
        "PRAGMA page_size"
    ).fetchone()[0]

    page_count = cursor.execute(
        "PRAGMA page_count"
    ).fetchone()[0]

    freelist_count = cursor.execute(
        "PRAGMA freelist_count"
    ).fetchone()[0]

    calculated_size = page_size * page_count
    free_space = page_size * freelist_count

    report.append("DATABASE INFORMATION")
    report.append("-" * 70)
    report.append(f"Page size: {page_size:,} bytes")
    report.append(f"Page count: {page_count:,}")
    report.append(f"Calculated DB size: {format_bytes(calculated_size)}")
    report.append(f"Free pages: {freelist_count:,}")
    report.append(f"Potential reusable/free space: {format_bytes(free_space)}")
    report.append("")

    # ========================================================
    # TABLE LIST
    # ========================================================

    print("Finding tables...")

    cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)

    tables = [row[0] for row in cursor.fetchall()]

    report.append("TABLE SUMMARY")
    report.append("-" * 70)
    report.append(f"Total application tables: {len(tables)}")
    report.append("")

    table_results = []

    for number, table in enumerate(tables, start=1):

        print(
            f"[{number}/{len(tables)}] Inspecting table: {table}"
        )

        quoted_table = safe_identifier(table)

        try:
            row_count = cursor.execute(
                f"SELECT COUNT(*) FROM {quoted_table}"
            ).fetchone()[0]

        except Exception as e:
            row_count = f"ERROR: {e}"

        table_results.append(
            {
                "name": table,
                "rows": row_count
            }
        )

    # Sort numeric row counts largest first

    numeric_tables = [
        item for item in table_results
        if isinstance(item["rows"], int)
    ]

    numeric_tables.sort(
        key=lambda x: x["rows"],
        reverse=True
    )

    failed_tables = [
        item for item in table_results
        if not isinstance(item["rows"], int)
    ]

    report.append("TABLES BY ROW COUNT")
    report.append("-" * 70)

    for item in numeric_tables:
        report.append(
            f"{item['name']:<45} {item['rows']:>15,} rows"
        )

    for item in failed_tables:
        report.append(
            f"{item['name']:<45} {item['rows']}"
        )

    report.append("")

    # ========================================================
    # DBSTAT STORAGE ANALYSIS
    # ========================================================

    print()
    print("Attempting per-table storage analysis...")

    dbstat_available = True

    try:
        cursor.execute(
            "SELECT name, SUM(pgsize) FROM dbstat GROUP BY name LIMIT 1"
        )
        cursor.fetchone()

    except Exception:
        dbstat_available = False

    if dbstat_available:

        report.append("APPROXIMATE STORAGE BY TABLE / INDEX")
        report.append("-" * 70)

        cursor.execute("""
            SELECT
                name,
                SUM(pgsize) AS total_bytes
            FROM dbstat
            GROUP BY name
            ORDER BY total_bytes DESC
        """)

        storage_rows = cursor.fetchall()

        for name, total_bytes in storage_rows:

            if total_bytes is None:
                total_bytes = 0

            report.append(
                f"{name:<45} {format_bytes(total_bytes):>15}"
            )

        report.append("")

    else:

        report.append("APPROXIMATE STORAGE BY TABLE / INDEX")
        report.append("-" * 70)
        report.append(
            "dbstat is not available in this SQLite build."
        )
        report.append(
            "Row counts are still included in this report."
        )
        report.append("")

    # ========================================================
    # TABLE SCHEMAS
    # ========================================================

    report.append("TABLE SCHEMAS")
    report.append("=" * 70)
    report.append("")

    for table in tables:

        report.append(f"TABLE: {table}")
        report.append("-" * 70)

        quoted_table = safe_identifier(table)

        try:

            columns = cursor.execute(
                f"PRAGMA table_info({quoted_table})"
            ).fetchall()

            for column in columns:

                cid = column[0]
                name = column[1]
                col_type = column[2]
                not_null = column[3]
                default = column[4]
                primary_key = column[5]

                report.append(
                    f"  {cid}: {name} | "
                    f"type={col_type or 'UNSPECIFIED'} | "
                    f"notnull={not_null} | "
                    f"default={default} | "
                    f"pk={primary_key}"
                )

        except Exception as e:

            report.append(
                f"Unable to read schema: {e}"
            )

        report.append("")

    # ========================================================
    # INDEX INFORMATION
    # ========================================================

    report.append("INDEXES")
    report.append("=" * 70)
    report.append("")

    cursor.execute("""
        SELECT
            name,
            tbl_name,
            sql
        FROM sqlite_master
        WHERE type = 'index'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY tbl_name, name
    """)

    indexes = cursor.fetchall()

    if indexes:

        for index_name, table_name, sql in indexes:

            report.append(
                f"INDEX: {index_name}"
            )

            report.append(
                f"TABLE: {table_name}"
            )

            report.append(
                f"SQL: {sql or '(automatic/internal definition)'}"
            )

            report.append("")

    else:

        report.append("No user-created indexes found.")
        report.append("")

    # ========================================================
    # POSSIBLE AI TABLES
    # ========================================================

    ai_keywords = [
        "ai",
        "ebook",
        "book",
        "chunk",
        "embedding",
        "vector",
        "index",
        "document",
        "resource",
        "sermon",
        "search",
        "rag",
        "library"
    ]

    possible_ai_tables = []

    for table in tables:

        lower_name = table.lower()

        if any(
            keyword in lower_name
            for keyword in ai_keywords
        ):
            possible_ai_tables.append(table)

    report.append("POSSIBLE AI / INDEXING TABLES")
    report.append("=" * 70)
    report.append("")

    if possible_ai_tables:

        for table in possible_ai_tables:

            matching = next(
                (
                    item
                    for item in table_results
                    if item["name"] == table
                ),
                None
            )

            if matching:

                rows = matching["rows"]

                if isinstance(rows, int):
                    rows = f"{rows:,}"

                report.append(
                    f"{table} - {rows} rows"
                )

    else:

        report.append(
            "No tables were identified from table names alone."
        )

    report.append("")

    # ========================================================
    # JOURNAL MODE
    # ========================================================

    try:

        journal_mode = cursor.execute(
            "PRAGMA journal_mode"
        ).fetchone()[0]

        report.append("SQLITE SETTINGS")
        report.append("=" * 70)
        report.append("")
        report.append(
            f"Journal mode: {journal_mode}"
        )

    except Exception as e:

        report.append(
            f"Could not determine journal mode: {e}"
        )

    report.append("")

    # ========================================================
    # WRITE REPORT
    # ========================================================

    conn.close()

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        file.write(
            "\n".join(report)
        )

    print()
    print("=" * 70)
    print("INSPECTION COMPLETE")
    print("=" * 70)
    print()
    print("No database data was modified.")
    print()
    print(f"Report created:")
    print(f"  {OUTPUT_FILE}")
    print()
    print(
        "Upload database_report.txt to ChatGPT."
    )
    print()


if __name__ == "__main__":
    main()