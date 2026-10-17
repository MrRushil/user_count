"""
Vasora AI - marketing site + public live-viewer counter, in one process.

Pure standard library, no dependencies.

  GET  /, /frames/...    Static files from ./public (the landing page and its
                          hero animation frames).

  POST /heartbeat        body: {"sid": "<random per-tab id>"}
                          Records "this session is still here" with a timestamp.
                          No auth - any visitor's browser calls this every ~10s.

  GET  /count?token=XXX  Returns {"count": N} = number of distinct session ids
                          heard from in the last STALE_AFTER seconds, but only
                          if token matches LIVE_TOKEN. Wrong/missing token gets
                          a 403, not the real number - this is what keeps the
                          count owner-only (a shared secret the owner puts in
                          the page URL, not a login check).

  GET  /health           {"ok": true}

Sessions are kept in memory only. That's fine for a simple live counter: if
the process restarts, the count just starts over at 0 and repopulates within
one heartbeat interval.

Run: PORT=8000 LIVE_TOKEN=some-long-random-string python3 server.py
"""
import hmac
import json
import mimetypes
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

PORT = int(os.environ.get("PORT", "8000"))
LIVE_TOKEN = os.environ.get("LIVE_TOKEN", "").strip()
STALE_AFTER = 25  # seconds; heartbeats fire every ~10s client-side
SID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
MAX_BODY = 1024  # a heartbeat body is ~50 bytes
MAX_SESSIONS = 50_000  # hard cap so junk sids can't exhaust memory
PRUNE_EVERY = 1.0  # seconds; avoid an O(n) sweep on every single heartbeat
PUBLIC_DIR = os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "public"))

mimetypes.add_type("image/webp", ".webp")

_lock = threading.Lock()
_sessions = {}  # sid -> last_seen (time.monotonic())
_last_prune = 0.0


def _prune_locked(force=False):
    global _last_prune
    now = time.monotonic()
    if not force and now - _last_prune < PRUNE_EVERY:
        return
    _last_prune = now
    cutoff = now - STALE_AFTER
    dead = [sid for sid, t in _sessions.items() if t < cutoff]
    for sid in dead:
        del _sessions[sid]


def _token_ok(token):
    return bool(LIVE_TOKEN) and hmac.compare_digest(
        token.encode("utf-8"), LIVE_TOKEN.encode("utf-8")
    )


def _static_path(route):
    """Map a URL path to a file under PUBLIC_DIR, or None if it escapes it."""
    rel = unquote(route).lstrip("/") or "index.html"
    full = os.path.realpath(os.path.join(PUBLIC_DIR, rel))
    if os.path.isdir(full):
        full = os.path.join(full, "index.html")
    if not full.startswith(PUBLIC_DIR + os.sep) or not os.path.isfile(full):
        return None
    return full


class Handler(BaseHTTPRequestHandler):
    server_version = "VasoraSite/1.0"
    timeout = 15  # drop idle/slow connections instead of tying up a thread

    def log_message(self, fmt, *args):
        pass  # keep stdout quiet; hosting platforms capture this anyway

    def _route(self):
        return urlsplit(self.path).path.rstrip("/") or "/"

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_file(self, path):
        with open(path, "rb") as f:
            body = f.read()
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        if ctype.startswith("text/"):
            ctype += "; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        # Frames never change; the page itself should pick up republishes.
        if path.endswith(".html"):
            self.send_header("Cache-Control", "no-cache")
        else:
            self.send_header("Cache-Control", "public, max-age=604800")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        route = self._route()
        if route == "/count":
            query = parse_qs(urlsplit(self.path).query)
            token = (query.get("token") or [""])[0]
            if not _token_ok(token):
                self._send_json(403, {"error": "forbidden"})
                return
            with _lock:
                _prune_locked(force=True)
                count = len(_sessions)
            self._send_json(200, {"count": count})
            return
        if route == "/health":
            self._send_json(200, {"ok": True})
            return
        path = _static_path(urlsplit(self.path).path)
        if path:
            self._send_file(path)
            return
        self._send_json(404, {"error": "not_found"})

    do_HEAD = do_GET

    def do_POST(self):
        if self._route() != "/heartbeat":
            self._send_json(404, {"error": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            length = -1
        if length < 0 or length > MAX_BODY:
            self.close_connection = True
            self._send_json(413 if length > MAX_BODY else 400, {"error": "bad_length"})
            return
        raw = self.rfile.read(length) if length else b""
        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, UnicodeDecodeError):
            data = {}
        sid = data.get("sid") if isinstance(data, dict) else None
        if not isinstance(sid, str) or not SID_RE.match(sid):
            self._send_json(400, {"error": "bad_sid"})
            return
        with _lock:
            _prune_locked()
            if sid not in _sessions and len(_sessions) >= MAX_SESSIONS:
                full = True
            else:
                _sessions[sid] = time.monotonic()
                full = False
        if full:
            self._send_json(503, {"error": "busy"})
            return
        self._send_json(200, {"ok": True})


def main():
    if not LIVE_TOKEN:
        print("WARNING: LIVE_TOKEN is not set - /count will always return 403.", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Vasora site + live counter listening on 0.0.0.0:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
