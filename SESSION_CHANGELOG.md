# Session Changelog — vision2030-v2

## Latest update — GCP migration + parallel scraping

### What changed
- **Google Cloud integration** (`utils/gcp_db.py`): GCS sync of SQLite DB, optional Cloud SQL (PostgreSQL via pg8000), optional Firestore. Auto-download on startup, throttled background uploads after writes.
- **Parallel scraping** (`engines/scraper_v30_advanced.py`): all 25 competitors scraped concurrently via `asyncio.Semaphore` + `asyncio.gather`. Streaming DB writes every 20 products to prevent loss on crash.
- **Live progress UI** (`app.py`): real-time counter shows `total_done / total_target`, prices found, errors, per-store breakdown. Auto-transitions to analysis dashboard on completion.
- **Auto-bootstrap of competitors** (`utils/db_manager.py`): reads `data/competitors_list_v30.json` on startup and registers 18 known competitors automatically.
- **Subprocess fix** (`app.py:4706`): added missing `--parallel-stores 25` flag so background scraper uses all stores instead of default 5.
- **Config** (`config.py`): added `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, `CLOUD_SQL_*`, `USE_FIRESTORE`, `GCP_ENABLED`.
- **Dependencies** (`requirements.txt`): `google-cloud-storage`, `google-cloud-firestore`, `cloud-sql-python-connector[pg8000]`, `SQLAlchemy`, `pg8000`.
- **Templates**: `.env.example` for environment variables, `.claude/launch.json` for dev server.
- **`.gitignore` hardening**: protects `.env`, service account JSONs, runtime scraper state files.

### Why
- Move local-only SQLite workflow to a cloud-backed setup without rewriting every query — GCS sync keeps existing SQLite logic intact while providing durability.
- Speed up data collection: previously sequential per-store scraping bottlenecked at 1 store at a time; now all 25 run in parallel.
- Prevent silent data loss when scraper crashes mid-run (streaming flushes).

### Reviewer notes
- All user-facing strings remain in Arabic; code comments in English.
- `anti_ban.py` rate limiting still active and unchanged.
- GCP features are opt-in via env vars — app continues to work fully on local SQLite if no GCP vars are set.
- `data/competitors_list_v30.json` is committed (whitelisted in `.gitignore`) so first-run bootstrap works out of the box.
