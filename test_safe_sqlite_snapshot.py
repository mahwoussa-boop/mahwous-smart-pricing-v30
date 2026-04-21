import os
import sqlite3
import tempfile
from utils.gcp_db import _create_sqlite_snapshot, _validate_sqlite_file

workdir = tempfile.mkdtemp(prefix="sqlite_wal_test_")
db_path = os.path.join(workdir, "test.db")
restored_path = os.path.join(workdir, "restored.db")

conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, name TEXT);")
for i in range(1, 6):
    conn.execute("INSERT INTO items(name) VALUES (?)", (f"item_{i}",))
conn.commit()

snapshot_path = _create_sqlite_snapshot(db_path)
_validate_sqlite_file(snapshot_path)
os.replace(snapshot_path, restored_path)

restored = sqlite3.connect(restored_path, timeout=30)
count = restored.execute("SELECT COUNT(*) FROM items").fetchone()[0]
rows = restored.execute("SELECT name FROM items ORDER BY id").fetchall()
restored.close()
conn.close()

print({
    "db_path": db_path,
    "restored_path": restored_path,
    "count": count,
    "rows": [r[0] for r in rows],
    "wal_exists": os.path.exists(db_path + "-wal"),
    "shm_exists": os.path.exists(db_path + "-shm"),
})
