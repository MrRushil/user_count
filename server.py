"""
Vasora AI - marketing site + public live-viewer counter, in one process.

Pure standard library, no dependencies.

  GET  /, /frames/...    Static files from ./public (the landing page and its
                          hero animation frames).

  POST /heartbeat        body: {"sid": "<random per-tab id>"}
                          Records "this session is still here" with a timestamp.
                          No auth - any visitor's browser calls this every ~10s.

  GET  /count?token=XXX  Returns {"count", "today", "week", "total", "day", "weekOf"}:
                          count = distinct tabs heard from in the last
                          STALE_AFTER seconds; today / week / total = distinct
                          visitors (per-browser "vid") since midnight, since
                          Monday, and all time, in DAY_TZ (total is a
                          HyperLogLog estimate, exact at small sizes). Only if
                          token matches LIVE_TOKEN. Wrong/missing token gets
                          a 403, not the real number - this is what keeps the
                          count owner-only (a shared secret the owner puts in
                          the page URL, not a login check).

  GET  /health           {"ok": true}

Live sessions are kept in memory only: if the process restarts, the live count
starts over and repopulates within one heartbeat interval. The daily visitor
set is kept in a Redis-compatible store (Render Key Value, KV_URL) so it
survives the free web service sleeping; without KV_URL it falls back to memory.

Run: PORT=8000 LIVE_TOKEN=some-long-random-string [KV_URL=redis://host:6379] python3 server.py
"""
import hmac
import json
import mimetypes
import os
import re
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

PORT = int(os.environ.get("PORT", "8000"))
LIVE_TOKEN = os.environ.get("LIVE_TOKEN", "").strip()
STALE_AFTER = 25  # seconds; heartbeats fire every ~10s client-side
SID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
MAX_BODY = 1024  # a heartbeat body is ~50 bytes
MAX_SESSIONS = 50_000  # hard cap so junk sids can't exhaust memory
PRUNE_EVERY = 1.0  # seconds; avoid an O(n) sweep on every single heartbeat
DAY_TZ = os.environ.get("DAY_TZ", "America/Los_Angeles")
KV_URL = os.environ.get("KV_URL", "").strip()
DAY_TTL = 8 * 24 * 3600  # keep a week of daily sets
WEEK_TTL = 15 * 24 * 3600  # keep last week's set around too
try:
    from zoneinfo import ZoneInfo
    _TZ = ZoneInfo(DAY_TZ)
except Exception:  # no tz database on the host: Pacific standard time is close enough
    _TZ = timezone(timedelta(hours=-8))
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


def _today():
    return datetime.now(_TZ).strftime("%Y-%m-%d")


def _week_of(day):
    """Monday (YYYY-MM-DD) of the week containing day, so weeks run Mon-Sun."""
    d = datetime.strptime(day, "%Y-%m-%d")
    return (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")


class KV:
    """Just enough of the Redis protocol (RESP) for the few commands used here."""

    def __init__(self, url):
        u = urlsplit(url)
        self.addr = (u.hostname, u.port or 6379)
        self.password = unquote(u.password) if u.password else None
        self.user = unquote(u.username) if u.username else None
        self.sock = None
        self.lock = threading.Lock()

    def _connect(self):
        s = socket.create_connection(self.addr, timeout=3)
        self.sock, self.rfile = s, s.makefile("rb")
        if self.password:
            self._send(["AUTH"] + ([self.user] if self.user else []) + [self.password])

    def _send(self, args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(out))
        return self._read()

    def _read(self):
        line = self.rfile.readline()
        if not line:
            raise ConnectionError("closed")
        kind, rest = line[:1], line[1:-2]
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"+":
            return rest.decode()
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self.rfile.read(n + 2)[:-2].decode()
        if kind == b"*":
            return [self._read() for _ in range(int(rest))]
        raise RuntimeError("bad reply")

    def call(self, *args):
        with self.lock:
            for attempt in (1, 2):  # reconnect once if the connection went stale
                try:
                    if self.sock is None:
                        self._connect()
                    return self._send(list(args))
                except (OSError, ConnectionError):
                    self.sock = None
                    if attempt == 2:
                        raise


_kv = KV(KV_URL) if KV_URL else None
ALL_KEY = "visitors:all"  # HyperLogLog: fixed ~12 KB however many visitors
_mem = {"day": None, "week": None, "today": set(), "thisweek": set(), "all": set()}  # fallback
MAX_MEM = 200_000


def _record_visit(vid):
    day = _today()
    week = _week_of(day)
    if _kv:
        try:
            dkey = "visitors:" + day
            if _kv.call("SADD", dkey, vid):  # first visit today: update week and all-time too
                _kv.call("EXPIRE", dkey, DAY_TTL)
                wkey = "visitors:week:" + week
                if _kv.call("SADD", wkey, vid):
                    _kv.call("EXPIRE", wkey, WEEK_TTL)
                _kv.call("PFADD", ALL_KEY, vid)
            return
        except Exception as e:
            print(f"KV write failed, using memory: {e}", flush=True)
    with _lock:
        if _mem["day"] != day:
            _mem["day"], _mem["today"] = day, set()
        if _mem["week"] != week:
            _mem["week"], _mem["thisweek"] = week, set()
        for k in ("today", "thisweek", "all"):
            if len(_mem[k]) < MAX_MEM:
                _mem[k].add(vid)


def _visitor_stats():
    day = _today()
    week = _week_of(day)
    if _kv:
        try:
            return {
                "day": day,
                "weekOf": week,
                "today": int(_kv.call("SCARD", "visitors:" + day) or 0),
                "week": int(_kv.call("SCARD", "visitors:week:" + week) or 0),
                "total": int(_kv.call("PFCOUNT", ALL_KEY) or 0),
            }
        except Exception as e:
            print(f"KV read failed, using memory: {e}", flush=True)
    with _lock:
        return {
            "day": day,
            "weekOf": week,
            "today": len(_mem["today"]) if _mem["day"] == day else 0,
            "week": len(_mem["thisweek"]) if _mem["week"] == week else 0,
            "total": len(_mem["all"]),
        }


def _backfill():
    """Fold the saved daily sets (kept 8 days) into this week's set and the
    all-time count, once, so the new totals don't start from zero."""
    if not _kv:
        return
    try:
        if not _kv.call("SET", "visitors:backfill:v1", "1", "NX"):
            return  # already done by an earlier start
        today = datetime.strptime(_today(), "%Y-%m-%d")
        week = _week_of(_today())
        for back in range(9):
            day = (today - timedelta(days=back)).strftime("%Y-%m-%d")
            members = _kv.call("SMEMBERS", "visitors:" + day) or []
            if not members:
                continue
            _kv.call("PFADD", ALL_KEY, *members)
            if _week_of(day) == week:
                _kv.call("SADD", "visitors:week:" + week, *members)
                _kv.call("EXPIRE", "visitors:week:" + week, WEEK_TTL)
        print("Backfilled weekly and all-time visitors from saved days", flush=True)
    except Exception as e:
        print(f"Backfill skipped: {e}", flush=True)


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
            stats = _visitor_stats()
            stats["count"] = count
            self._send_json(200, stats)
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
        vid = data.get("vid")
        _record_visit(vid if isinstance(vid, str) and SID_RE.match(vid) else sid)
        self._send_json(200, {"ok": True})


def main():
    if not LIVE_TOKEN:
        print("WARNING: LIVE_TOKEN is not set - /count will always return 403.", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Vasora site + live counter listening on 0.0.0.0:{PORT}", flush=True)
    print("Visitor totals stored in " + ("Key Value store" if _kv else "memory (set KV_URL to persist)"), flush=True)
    threading.Thread(target=_backfill, daemon=True).start()
    server.serve_forever()


if __name__ == "__main__":
    main()
