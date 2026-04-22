#!/usr/bin/env python3
"""
docker_entrypoint.py — نقطة دخول الحاوية لمهووس v30

Architecture:
  Cloud Run (PORT=8080)
      ↓
  nginx (listens on $PORT, adds Cache-Control: no-store to HTML)
      ↓
  Streamlit (listens on 127.0.0.1:8501, internal only)

WHY nginx?
  After every Cloud Run redeployment, Streamlit re-hashes its JS bundles
  (e.g. index.ByklcDol.js → index.DLPgdyUk.js).  If the browser has the
  OLD HTML cached it tries to load old chunk URLs that no longer exist:
      TypeError: Failed to fetch dynamically imported module
  The fix is simple: tell the browser NEVER to cache the HTML entry-point.
  nginx intercepts the Streamlit HTML response and adds:
      Cache-Control: no-cache, no-store, must-revalidate
  So every page open fetches fresh HTML with the current JS hashes.

  st.markdown("<script>") cannot fix this — React's dangerouslySetInnerHTML
  does not execute injected <script> tags, and the error fires before Python
  even starts running.
"""
import os
import signal
import subprocess
import sys
import time
import base64
from pathlib import Path
from urllib.request import urlretrieve


# ══════════════════════════════════════════════════════════════════════════════
#  Data-restoration helpers (unchanged from original)
# ══════════════════════════════════════════════════════════════════════════════

_B64_FILES = {
    "CATEGORIES_CSV_B64":    "categories.csv",
    "OUR_CATALOG_CSV_B64":   "our_catalog.csv",
    "COMPETITORS_JSON_B64":  "competitors_list.json",
    "SALLA_BRANDS_B64":      "ماركات مهووس.csv",
    "SALLA_CATEGORIES_B64":  "تصنيفات مهووس.csv",
}

_URL_FILES = {
    "BRANDS_CSV_URL":        "brands.csv",
    "SALLA_BRANDS_URL":      "ماركات مهووس.csv",
    "SALLA_CATEGORIES_URL":  "تصنيفات مهووس.csv",
    "OUR_CATALOG_CSV_URL":   "our_catalog.csv",
    "COMPETITORS_JSON_URL":  "competitors_list.json",
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
        print(f"[entrypoint] ⚠️ تعذّر استخدام DATA_DIR='{data_dir}' بسبب: {e} — سيتم استخدام {fallback}")
        return fallback


def _restore_data_files() -> None:
    data_dir = _resolve_startup_data_dir()

    for env_key, filename in _B64_FILES.items():
        b64_val = (os.environ.get(env_key) or "").strip()
        if not b64_val:
            continue
        dest = os.path.join(data_dir, filename)
        if os.path.exists(dest):
            print(f"[entrypoint] ℹ️ موجود (تخطي): {filename}")
            continue
        try:
            with open(dest, "wb") as fh:
                fh.write(base64.b64decode(b64_val))
            sz = os.path.getsize(dest)
            print(f"[entrypoint] ✅ Base64 → {filename} ({sz:,} bytes)")
        except Exception as e:
            print(f"[entrypoint] ❌ فشل Base64 {env_key}: {e}")

    if not _env_truthy("RESTORE_URL_FILES_ON_STARTUP"):
        if any((os.environ.get(k) or "").strip() for k in _URL_FILES):
            print("[entrypoint] ℹ️ تم تجاهل تنزيلات URL عند الإقلاع. فعّل RESTORE_URL_FILES_ON_STARTUP=1 إذا أردت استعادتها قبل البدء.")
        return

    for env_key, filename in _URL_FILES.items():
        url = (os.environ.get(env_key) or "").strip()
        if not url:
            continue
        dest = os.path.join(data_dir, filename)
        if os.path.exists(dest):
            print(f"[entrypoint] ℹ️ موجود (تخطي): {filename}")
            continue
        try:
            print(f"[entrypoint] ⬇️ تحميل {filename} من URL...")
            urlretrieve(url, dest)
            sz = os.path.getsize(dest)
            print(f"[entrypoint] ✅ URL → {filename} ({sz:,} bytes)")
        except Exception as e:
            print(f"[entrypoint] ❌ فشل تحميل {env_key}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  Port helpers
# ══════════════════════════════════════════════════════════════════════════════

def _cloud_port() -> int:
    """External port Cloud Run exposes (default 8080)."""
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
#  nginx configuration generator
# ══════════════════════════════════════════════════════════════════════════════

_NGINX_CONF_TEMPLATE = """\
# Auto-generated by docker_entrypoint.py — do not edit manually.
# Proxy: Cloud Run :{ext_port} → Streamlit 127.0.0.1:{st_port}
# Purpose: add Cache-Control: no-store to HTML so browsers always fetch
#          fresh HTML after a redeployment (fixes "Failed to fetch
#          dynamically imported module" / stale JS chunk hash errors).

worker_processes 1;
pid /tmp/nginx_mahwous.pid;
error_log /tmp/nginx_mahwous_error.log warn;

events {{
    worker_connections 512;
    use epoll;
}}

http {{
    access_log /tmp/nginx_mahwous_access.log;

    # Allow large file uploads (match Streamlit's maxUploadSize = 200 MB)
    client_max_body_size 210m;

    # WebSocket connection upgrade map
    map $http_upgrade $connection_upgrade {{
        default upgrade;
        ''      close;
    }}

    upstream streamlit_backend {{
        server 127.0.0.1:{st_port};
        keepalive 32;
    }}

    server {{
        listen {ext_port};

        # ── HTML entry-point: NEVER cache ─────────────────────────────────
        # This is the root cause of "Failed to fetch dynamically imported
        # module". If the browser caches the HTML it will reference old JS
        # chunk hashes after redeployment. Telling it no-store fixes this.
        location = / {{
            proxy_pass         http://streamlit_backend;
            proxy_set_header   Host              $host;
            proxy_set_header   X-Real-IP         $remote_addr;
            proxy_http_version 1.1;
            proxy_set_header   Upgrade           $http_upgrade;
            proxy_set_header   Connection        $connection_upgrade;
            proxy_read_timeout 300s;

            # Cache-busting headers on the HTML response only
            add_header Cache-Control "no-cache, no-store, must-revalidate" always;
            add_header Pragma        "no-cache"                            always;
            add_header Expires       "0"                                   always;
        }}

        # ── Streamlit WebSocket stream ─────────────────────────────────────
        location /_stcore/stream {{
            proxy_pass          http://streamlit_backend;
            proxy_http_version  1.1;
            proxy_set_header    Upgrade    $http_upgrade;
            proxy_set_header    Connection "upgrade";
            proxy_set_header    Host       $host;
            proxy_read_timeout  86400s;
            proxy_send_timeout  86400s;
        }}

        # ── Health check (Cloud Run probes this) ───────────────────────────
        location /_stcore/health {{
            proxy_pass         http://streamlit_backend;
            proxy_set_header   Host $host;
            proxy_read_timeout 10s;
        }}

        # ── Everything else (static JS/CSS assets, API calls) ─────────────
        location / {{
            proxy_pass         http://streamlit_backend;
            proxy_set_header   Host              $host;
            proxy_set_header   X-Real-IP         $remote_addr;
            proxy_http_version 1.1;
            proxy_set_header   Upgrade           $http_upgrade;
            proxy_set_header   Connection        $connection_upgrade;
            proxy_read_timeout 300s;
            proxy_send_timeout 300s;
        }}
    }}
}}
"""

_NGINX_CONF_PATH = "/tmp/mahwous_nginx.conf"


def _write_nginx_conf(ext_port: int, st_port: int) -> None:
    conf = _NGINX_CONF_TEMPLATE.format(ext_port=ext_port, st_port=st_port)
    with open(_NGINX_CONF_PATH, "w") as fh:
        fh.write(conf)
    print(f"[entrypoint] ✅ nginx config written → {_NGINX_CONF_PATH}")


# ══════════════════════════════════════════════════════════════════════════════
#  Streamlit readiness probe
# ══════════════════════════════════════════════════════════════════════════════

def _wait_for_streamlit(port: int, timeout: int = 60) -> bool:
    """Poll /_stcore/health until Streamlit is up or timeout expires."""
    import urllib.request
    url = f"http://127.0.0.1:{port}/_stcore/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if r.status == 200:
                    print(f"[entrypoint] ✅ Streamlit ready on :{port}")
                    return True
        except Exception:
            pass
        time.sleep(1)
    print(f"[entrypoint] ⚠️ Streamlit did not respond within {timeout}s — starting nginx anyway")
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # Step 1: restore data files from env vars
    _restore_data_files()

    ext_port   = _cloud_port()   # 8080 — what Cloud Run exposes
    st_port    = 8501            # internal only, never exposed

    _strip_broken_streamlit_server_env()

    # Step 2: write nginx proxy config
    _write_nginx_conf(ext_port, st_port)

    # Step 3: start Streamlit on the internal port (127.0.0.1 only)
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

    # Step 4: wait for Streamlit to be ready before accepting traffic
    _wait_for_streamlit(st_port, timeout=90)

    # Step 5: start nginx in the foreground (daemon off) to handle ext_port
    print(f"[entrypoint] 🌐 Starting nginx on :{ext_port} (proxy → Streamlit :{st_port}) ...")
    nginx_proc = subprocess.Popen(
        ["nginx", "-c", _NGINX_CONF_PATH, "-g", "daemon off;"],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    # Step 6: forward OS signals to both children
    def _forward_signal(signum, frame):
        for proc in (streamlit_proc, nginx_proc):
            try:
                proc.send_signal(signum)
            except Exception:
                pass

    signal.signal(signal.SIGTERM, _forward_signal)
    signal.signal(signal.SIGINT,  _forward_signal)

    # Step 7: supervise — restart neither, just exit if either dies
    try:
        while True:
            st_rc  = streamlit_proc.poll()
            ng_rc  = nginx_proc.poll()

            if st_rc is not None:
                print(f"[entrypoint] ❌ Streamlit exited (rc={st_rc}) — shutting down")
                nginx_proc.terminate()
                sys.exit(st_rc)

            if ng_rc is not None:
                print(f"[entrypoint] ❌ nginx exited (rc={ng_rc}) — shutting down")
                streamlit_proc.terminate()
                sys.exit(ng_rc)

            time.sleep(2)

    except KeyboardInterrupt:
        print("[entrypoint] 🛑 Interrupted — stopping both processes")
        streamlit_proc.terminate()
        nginx_proc.terminate()
        streamlit_proc.wait()
        nginx_proc.wait()


if __name__ == "__main__":
    main()
