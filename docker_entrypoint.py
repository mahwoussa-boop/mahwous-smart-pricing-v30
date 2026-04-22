#!/usr/bin/env python3
"""
docker_entrypoint.py — نقطة دخول الحاوية لمهووس v30

Architecture:
  Cloud Run (PORT=8080)  →  nginx (8080)  →  Streamlit (127.0.0.1:8501)

WHY nginx?
  Every Cloud Run deployment regenerates Streamlit JS chunk hashes.
  If the browser has OLD HTML cached it tries to load old chunk URLs
  that no longer exist:
      TypeError: Failed to fetch dynamically imported module

  nginx adds:  Cache-Control: no-cache, no-store, must-revalidate
  on every HTML response.  st.markdown(<script>) cannot fix this because
  React's dangerouslySetInnerHTML never executes injected <script> tags.

STARTUP ORDER (critical for Cloud Run):
  1. nginx starts FIRST on $PORT=8080 — Cloud Run health probe gets 200
     immediately from nginx's own /health stub (no Streamlit needed yet).
  2. Streamlit starts on 127.0.0.1:8501 (internal, never exposed).
  3. Once Streamlit is up, nginx begins forwarding real traffic.
  User may see a brief 502 during the ~5 s Streamlit cold-start window;
  that is acceptable and far better than an infinite "Failed to fetch" loop.
"""
import base64
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen, Request


# ══════════════════════════════════════════════════════════════════════════════
#  Data-restoration helpers (unchanged from original)
# ══════════════════════════════════════════════════════════════════════════════

_B64_FILES = {
    "CATEGORIES_CSV_B64":   "categories.csv",
    "OUR_CATALOG_CSV_B64":  "our_catalog.csv",
    "COMPETITORS_JSON_B64": "competitors_list.json",
    "SALLA_BRANDS_B64":     "ماركات مهووس.csv",
    "SALLA_CATEGORIES_B64": "تصنيفات مهووس.csv",
}
_URL_FILES = {
    "BRANDS_CSV_URL":       "brands.csv",
    "SALLA_BRANDS_URL":     "ماركات مهووس.csv",
    "SALLA_CATEGORIES_URL": "تصنيفات مهووس.csv",
    "OUR_CATALOG_CSV_URL":  "our_catalog.csv",
    "COMPETITORS_JSON_URL": "competitors_list.json",
}


def _default_data_dir() -> str:
    return str((Path(__file__).resolve().parent / "data").resolve())


def _env_truthy(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_startup_data_dir() -> str:
    requested = (os.environ.get("DATA_DIR") or "").strip()
    data_dir = requested or _default_data_dir()
    try:
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    except Exception as e:
        fallback = _default_data_dir()
        os.makedirs(fallback, exist_ok=True)
        os.environ["DATA_DIR"] = fallback
        print(f"[entrypoint] ⚠️  DATA_DIR='{data_dir}' failed: {e} — using {fallback}")
        return fallback


def _restore_data_files() -> None:
    data_dir = _resolve_startup_data_dir()
    for env_key, filename in _B64_FILES.items():
        b64_val = (os.environ.get(env_key) or "").strip()
        if not b64_val:
            continue
        dest = os.path.join(data_dir, filename)
        if os.path.exists(dest):
            print(f"[entrypoint] ℹ️  skip (exists): {filename}")
            continue
        try:
            with open(dest, "wb") as fh:
                fh.write(base64.b64decode(b64_val))
            print(f"[entrypoint] ✅ base64 → {filename} ({os.path.getsize(dest):,} B)")
        except Exception as e:
            print(f"[entrypoint] ❌ base64 {env_key}: {e}")

    if not _env_truthy("RESTORE_URL_FILES_ON_STARTUP"):
        if any((os.environ.get(k) or "").strip() for k in _URL_FILES):
            print("[entrypoint] ℹ️  URL restores skipped (set RESTORE_URL_FILES_ON_STARTUP=1 to enable)")
        return

    from urllib.request import urlretrieve
    for env_key, filename in _URL_FILES.items():
        url = (os.environ.get(env_key) or "").strip()
        if not url:
            continue
        dest = os.path.join(data_dir, filename)
        if os.path.exists(dest):
            print(f"[entrypoint] ℹ️  skip (exists): {filename}")
            continue
        try:
            print(f"[entrypoint] ⬇️  downloading {filename} ...")
            urlretrieve(url, dest)
            print(f"[entrypoint] ✅ URL → {filename} ({os.path.getsize(dest):,} B)")
        except Exception as e:
            print(f"[entrypoint] ❌ URL {env_key}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  Port helpers
# ══════════════════════════════════════════════════════════════════════════════

def _cloud_port() -> int:
    """PORT injected by Cloud Run (default 8080)."""
    raw = (os.environ.get("PORT") or "").strip() or "8080"
    try:
        p = int(raw)
        if 1 <= p <= 65535:
            return p
    except ValueError:
        pass
    return 8080


def _strip_broken_streamlit_server_env() -> None:
    for key in list(os.environ):
        if key.startswith("STREAMLIT_SERVER_"):
            os.environ.pop(key, None)


# ══════════════════════════════════════════════════════════════════════════════
#  nginx config
# ══════════════════════════════════════════════════════════════════════════════

_NGINX_CONF_PATH = "/tmp/mahwous_nginx.conf"

_NGINX_CONF = """\
# Auto-generated — do not edit.
# Proxy: Cloud Run :{ext_port}  →  Streamlit 127.0.0.1:{st_port}
worker_processes 1;
pid /tmp/nginx_mahwous.pid;
error_log /tmp/nginx_error.log warn;

events {{ worker_connections 512; use epoll; }}

http {{
    access_log /tmp/nginx_access.log;
    client_max_body_size 210m;

    # Handle WebSocket upgrade header
    map $http_upgrade $conn_upgrade {{
        default upgrade;
        ''      close;
    }}

    upstream st_backend {{ server 127.0.0.1:{st_port}; keepalive 16; }}

    server {{
        listen {ext_port};

        # ── Cloud Run startup probe ────────────────────────────────────────
        # nginx answers /_stcore/health IMMEDIATELY so Cloud Run marks the
        # container healthy before Streamlit finishes loading (~5 s).
        # Real user traffic will get 502 for those few seconds — acceptable.
        location = /_stcore/health {{
            access_log off;
            return 200 "ok\\n";
            add_header Content-Type text/plain;
        }}

        # ── HTML entry-point: no-cache ─────────────────────────────────────
        # Browsers must not cache the HTML page. If they do, after a new
        # deployment they will reference old JS chunk hashes that no longer
        # exist →  TypeError: Failed to fetch dynamically imported module.
        location = / {{
            proxy_pass         http://st_backend;
            proxy_http_version 1.1;
            proxy_set_header   Host       $host;
            proxy_set_header   Upgrade    $http_upgrade;
            proxy_set_header   Connection $conn_upgrade;
            proxy_read_timeout 300s;
            add_header Cache-Control "no-cache, no-store, must-revalidate" always;
            add_header Pragma        "no-cache"                            always;
            add_header Expires       "0"                                   always;
        }}

        # ── Streamlit WebSocket ────────────────────────────────────────────
        location /_stcore/stream {{
            proxy_pass          http://st_backend;
            proxy_http_version  1.1;
            proxy_set_header    Upgrade    $http_upgrade;
            proxy_set_header    Connection "upgrade";
            proxy_set_header    Host       $host;
            proxy_read_timeout  86400s;
            proxy_send_timeout  86400s;
        }}

        # ── Everything else (JS/CSS assets, API endpoints) ────────────────
        location / {{
            proxy_pass         http://st_backend;
            proxy_http_version 1.1;
            proxy_set_header   Host       $host;
            proxy_set_header   Upgrade    $http_upgrade;
            proxy_set_header   Connection $conn_upgrade;
            proxy_read_timeout 300s;
            proxy_send_timeout 300s;
        }}
    }}
}}
"""


def _write_nginx_conf(ext_port: int, st_port: int) -> None:
    with open(_NGINX_CONF_PATH, "w") as fh:
        fh.write(_NGINX_CONF.format(ext_port=ext_port, st_port=st_port))
    print(f"[entrypoint] ✅ nginx config → {_NGINX_CONF_PATH}")


# ══════════════════════════════════════════════════════════════════════════════
#  Streamlit readiness probe
# ══════════════════════════════════════════════════════════════════════════════

def _wait_for_streamlit(port: int, timeout: int = 90) -> bool:
    """Poll Streamlit's health endpoint until it responds 200 or timeout."""
    url = f"http://127.0.0.1:{port}/_stcore/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urlopen(Request(url), timeout=2) as r:
                if r.status == 200:
                    print(f"[entrypoint] ✅ Streamlit ready on :{port}")
                    return True
        except Exception:
            pass
        time.sleep(1)
    print(f"[entrypoint] ⚠️  Streamlit not ready after {timeout}s — nginx will 502 until it is")
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # ── Step 1: restore data files from env vars ──────────────────────────
    _restore_data_files()

    ext_port = _cloud_port()   # 8080 — exposed by Cloud Run
    st_port  = 8501            # internal only, bound to 127.0.0.1

    _strip_broken_streamlit_server_env()

    # ── Step 2: write nginx config ────────────────────────────────────────
    _write_nginx_conf(ext_port, st_port)

    # ── Step 3: START nginx FIRST on $PORT ────────────────────────────────
    # Cloud Run checks that $PORT is listening before routing traffic.
    # nginx starts in <1 s and immediately answers /_stcore/health → 200,
    # so the container is marked healthy right away.
    print(f"[entrypoint] 🌐 Starting nginx on :{ext_port} ...")
    nginx_proc = subprocess.Popen(
        ["nginx", "-c", _NGINX_CONF_PATH, "-g", "daemon off;"],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    # ── Step 4: start Streamlit on internal port ──────────────────────────
    print(f"[entrypoint] 🚀 Starting Streamlit on 127.0.0.1:{st_port} ...")
    streamlit_proc = subprocess.Popen(
        [
            "streamlit", "run", "app.py",
            "--server.port",    str(st_port),
            "--server.address", "127.0.0.1",
            "--server.headless","true",
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    # ── Step 5: wait for Streamlit (nginx 502s are benign while it loads) ─
    _wait_for_streamlit(st_port, timeout=90)

    # ── Step 6: forward OS signals to both children ───────────────────────
    def _fwd_signal(signum, _frame):
        for p in (nginx_proc, streamlit_proc):
            try:
                p.send_signal(signum)
            except Exception:
                pass

    signal.signal(signal.SIGTERM, _fwd_signal)
    signal.signal(signal.SIGINT,  _fwd_signal)

    # ── Step 7: supervise — exit if either child dies ─────────────────────
    try:
        while True:
            ng_rc = nginx_proc.poll()
            st_rc = streamlit_proc.poll()
            if ng_rc is not None:
                print(f"[entrypoint] ❌ nginx exited (rc={ng_rc}) — stopping")
                streamlit_proc.terminate()
                sys.exit(ng_rc)
            if st_rc is not None:
                print(f"[entrypoint] ❌ Streamlit exited (rc={st_rc}) — stopping")
                nginx_proc.terminate()
                sys.exit(st_rc)
            time.sleep(2)
    except KeyboardInterrupt:
        nginx_proc.terminate()
        streamlit_proc.terminate()
        nginx_proc.wait()
        streamlit_proc.wait()


if __name__ == "__main__":
    main()
