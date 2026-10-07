import sqlite3
import os

DB_PATH = "app_v2.db"

if not os.path.exists(DB_PATH):
    print(f"ERROR: {DB_PATH} was not found.")
    raise SystemExit(1)

print(f"Opening: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Show size before cleanup
size_before = os.path.getsize(DB_PATH)
print(f"Database size before cleanup: {size_before / 1024 / 1024:.2f} MB")

print("\nRemoving OLD AI index tables from main database...")

# FTS table first
cursor.execute("DROP TABLE IF EXISTS pij_library_chunks_fts")
print("✓ Removed pij_library_chunks_fts")

# Chunk data
cursor.execute("DROP TABLE IF EXISTS pij_library_chunks")
print("✓ Removed pij_library_chunks")

# Document index
cursor.execute("DROP TABLE IF EXISTS pij_library_documents")
print("✓ Removed pij_library_documents")

conn.commit()

print("\nOld AI index tables removed.")
print("Running VACUUM to reclaim disk space...")

cursor.execute("VACUUM")

conn.close()

size_after = os.path.getsize(DB_PATH)

print("\n====================================")
print("CLEANUP COMPLETE")
print("====================================")
print(f"Before: {size_before / 1024 / 1024:.2f} MB")
print(f"After : {size_after / 1024 / 1024:.2f} MB")
print(
    f"Freed : {(size_before - size_after) / 1024 / 1024:.2f} MB"
)
print("====================================")