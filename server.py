"""
Vasora AI - public live-viewer counter.

Pure standard library, no dependencies. Two endpoints:

  POST /heartbeat        body: {"sid": "<random per-tab id>"}
                          Records "this session is still here" with a timestamp.
                          No auth - any visitor's browser calls this every ~10s.

  GET  /count?token=XXX  Returns {"count": N} = number of distinct session ids
                          heard from in the last STALE_AFTER seconds, but only
                          if token matches LIVE_TOKEN. Wrong/missing token gets
                          a 403, not the real number - this is what keeps the
                          count owner-only (a shared secret baked into the
                          landing page's JS, not a login check).

Sessions are kept in memory only. That's fine for a simple live counter: if
the process restarts, the count just starts over at 0 and repopulates within
one heartbeat interval.

Run: PORT=8000 LIVE_TOKEN=some-long-random-string python3 server.py
"""
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8000"))
LIVE_TOKEN = os.environ.get("LIVE_TOKEN", "")
STALE_AFTER = 25  # seconds; heartbeats fire every ~10s client-side
SID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

_lock = threading.Lock()
_sessions = {}  # sid -> last_seen_unix_time


def _prune_locked():
    cutoff = time.time() - STALE_AFTER
    dead = [sid for sid, t in _sessions.items() if t < cutoff]
    for sid in dead:
        del _sessions[sid]


class Handler(BaseHTTPRequestHandler):
    server_version = "VasoraLiveCounter/1.0"

    def log_message(self, fmt, *args):
        pass  # keep stdout quiet; hosting platforms capture this anyway

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/count"):
            query = self.path.split("?", 1)[1] if "?" in self.path else ""
            params = dict(
                p.split("=", 1) for p in query.split("&") if "=" in p
            )
            token = params.get("token", "")
            if not LIVE_TOKEN or token != LIVE_TOKEN:
                self._send_json(403, {"error": "forbidden"})
                return
            with _lock:
                _prune_locked()
                count = len(_sessions)
            self._send_json(200, {"count": count})
            return
        if self.path.startswith("/health"):
            self._send_json(200, {"ok": True})
            return
        self._send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path.startswith("/heartbeat"):
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length else b""
            try:
                data = json.loads(raw.decode("utf-8")) if raw else {}
            except (ValueError, UnicodeDecodeError):
                data = {}
            sid = str(data.get("sid", ""))
            if not SID_RE.match(sid):
                self._send_json(400, {"error": "bad_sid"})
                return
            with _lock:
                _sessions[sid] = time.time()
                _prune_locked()
            self._send_json(200, {"ok": True})
            return
        self._send_json(404, {"error": "not_found"})


def main():
    if not LIVE_TOKEN:
        print("WARNING: LIVE_TOKEN is not set - /count will always return 403.")
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Vasora live counter listening on 0.0.0.0:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
