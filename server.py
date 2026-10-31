"""
Vasora AI - marketing site + public live-viewer counter, in one process.

Standard library only, except the anthropic package for POST /analyze.

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

  POST /waitlist         body: {"email": "..."} sent by the page after a successful
                          waitlist signup. Stores only a keyed hash of the
                          address (the emails themselves live in the inbox), so
                          /count can report how many distinct people joined.

  POST /queue            body: {"fields": {...}} - the same fields the page sends to
                          FormSubmit. The page calls this only when FormSubmit
                          itself is down; the form is held in Key Value
                          (forms:pending) and a background thread retries the
                          delivery every RETRY_EVERY seconds, deleting it once
                          FormSubmit accepts it. /count reports how many wait.

  POST /analyze          body: {"image": "<base64>", "media_type": "image/jpeg",
                          "profile": {...}, "vid": "...", "token": "..."}. Sends
                          the fridge photo and the visitor's answers to Claude
                          Haiku and returns {"readout": {...}}. One free readout
                          per visitor (by IP and by browser id); anyone with a
                          viewer token (LIVE_TOKEN or VIEWER_TOKENS) is
                          unlimited. Needs ANTHROPIC_API_KEY and the anthropic
                          package; without them it answers 503 and the page
                          falls back to its built-in sample readout.

  GET  /health           {"ok": true}

Live sessions are kept in memory only: if the process restarts, the live count
starts over and repopulates within one heartbeat interval. The daily visitor
set is kept in a Redis-compatible store (Render Key Value, KV_URL) so it
survives the free web service sleeping; without KV_URL it falls back to memory.

Run: PORT=8000 LIVE_TOKEN=some-long-random-string [KV_URL=redis://host:6379] python3 server.py
"""
import hashlib
import hmac
import json
import mimetypes
import os
import re
import socket
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

PORT = int(os.environ.get("PORT", "8000"))
LIVE_TOKEN = os.environ.get("LIVE_TOKEN", "").strip()
# Extra viewer links (co-founders): comma-separated tokens; remove one to revoke it.
VIEWER_TOKENS = [t.strip() for t in os.environ.get("VIEWER_TOKENS", "").split(",") if t.strip()]
PREVIEW_MODEL = os.environ.get("PREVIEW_MODEL", "claude-haiku-5-5")
PREVIEW_DAILY_CAP = int(os.environ.get("PREVIEW_DAILY_CAP", "300"))  # free readouts per day, all visitors
MAX_IMAGE_BODY = 1_500_000  # the page shrinks photos to ~1024px JPEG, well under this
STALE_AFTER = 25  # seconds; heartbeats fire every ~10s client-side
SID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
MAX_BODY = 1024  # a heartbeat body is ~50 bytes
MAX_SESSIONS = 50_000  # hard cap so junk sids can't exhaust memory
PRUNE_EVERY = 1.0  # seconds; avoid an O(n) sweep on every single heartbeat
DAY_TZ = os.environ.get("DAY_TZ", "America/Los_Angeles")
KV_URL = os.environ.get("KV_URL", "").strip()
FORM_ENDPOINT = os.environ.get("FORM_ENDPOINT", "https://formsubmit.co/ajax/vasora.ai@gmail.com")
SITE_ORIGIN = os.environ.get("SITE_ORIGIN", "https://www.vasorahealth.net")  # the origin FormSubmit activated
RETRY_EVERY = int(os.environ.get("RETRY_EVERY", "120"))
MAX_FORM_BODY = 8192  # a contact message can be a few KB
FORM_FIELDS = {"_subject", "_replyto", "_template", "_captcha", "name", "email", "topic", "message", "signup"}
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


WAITLIST_KEY = "waitlist:signups"
EMAIL_RE = re.compile(r"^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{2,24}$")
_waitlist_mem = set()


def _email_fingerprint(email):
    """Keyed one-way hash: counts each address once without storing it."""
    key = (LIVE_TOKEN or "vasora-waitlist").encode()
    return hmac.new(key, email.strip().lower().encode(), hashlib.sha256).hexdigest()[:32]


def _record_signup(email):
    fp = _email_fingerprint(email)
    if _kv:
        try:
            _kv.call("SADD", WAITLIST_KEY, fp)
            return
        except Exception as e:
            print(f"KV write failed, using memory: {e}", flush=True)
    with _lock:
        if len(_waitlist_mem) < MAX_MEM:
            _waitlist_mem.add(fp)


def _waitlist_count():
    if _kv:
        try:
            return int(_kv.call("SCARD", WAITLIST_KEY) or 0)
        except Exception as e:
            print(f"KV read failed, using memory: {e}", flush=True)
    with _lock:
        return len(_waitlist_mem)


PENDING_KEY = "forms:pending"
MAX_PENDING = 5000
_pending_mem = []


def _queue_form(fields):
    item = json.dumps({"fields": fields, "queued": int(time.time())})
    if _kv:
        try:
            if int(_kv.call("LLEN", PENDING_KEY) or 0) >= MAX_PENDING:
                return False
            _kv.call("RPUSH", PENDING_KEY, item)
            return True
        except Exception as e:
            print(f"KV write failed, using memory: {e}", flush=True)
    with _lock:
        if len(_pending_mem) >= MAX_PENDING:
            return False
        _pending_mem.append(item)
        return True


def _pending():
    if _kv:
        try:
            return list(_kv.call("LRANGE", PENDING_KEY, 0, 49) or [])
        except Exception as e:
            print(f"KV read failed, using memory: {e}", flush=True)
    with _lock:
        return list(_pending_mem[:50])


def _pending_count():
    if _kv:
        try:
            return int(_kv.call("LLEN", PENDING_KEY) or 0)
        except Exception:
            pass
    with _lock:
        return len(_pending_mem)


def _drop_pending(item):
    if _kv:
        try:
            _kv.call("LREM", PENDING_KEY, 1, item)
            return
        except Exception as e:
            print(f"KV delete failed: {e}", flush=True)
    with _lock:
        if item in _pending_mem:
            _pending_mem.remove(item)


def _deliver(fields):
    """Send one held form to FormSubmit, posing as the activated site origin.
    True only when FormSubmit says it accepted the submission."""
    body = json.dumps(dict({"_template": "table", "_captcha": "false"}, **fields)).encode()
    req = urllib.request.Request(FORM_ENDPOINT, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": SITE_ORIGIN,
        "Referer": SITE_ORIGIN + "/",
        "User-Agent": "Mozilla/5.0 (compatible; VasoraSite/1.0; +" + SITE_ORIGIN + ")",
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            reply = json.loads(r.read().decode("utf-8") or "{}")
        return str(reply.get("success")) == "true", reply.get("message", "")
    except Exception as e:
        return False, str(e)


def _retry_loop():
    while True:
        time.sleep(RETRY_EVERY)
        for item in _pending():
            try:
                fields = json.loads(item)["fields"]
            except Exception:
                _drop_pending(item)  # unreadable entry: nothing to deliver
                continue
            ok, msg = _deliver(fields)
            if ok:
                _drop_pending(item)
                print("Delivered a held form to FormSubmit", flush=True)
            else:
                print(f"FormSubmit still failing ({msg[:120]}); will retry", flush=True)
                break  # it's down: try the rest next round


# ---- AI preview (POST /analyze) ----

PREVIEW_USED_KEY = "preview:used"
_preview_used_mem = set()
_preview_day_mem = {}
IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp"}
CONDITIONS = {"Type 1 diabetes", "Type 2 diabetes", "Hypertension", "High body fat"}
GOALS = {"Lose weight", "Lose body fat", "Maintain weight", "No weight goal", ""}

# Accept the key under a couple of likely names, so a near-miss in the dashboard still works.
_KEY_NAMES = ("ANTHROPIC_API_KEY", "CLAUDE_API_KEY", "ANTHROPIC_KEY", "CLAUDE_KEY")
_API_KEY = next((os.environ[k].strip() for k in _KEY_NAMES if os.environ.get(k, "").strip()), "")
_PREVIEW_OFF_WHY = ""
try:
    import anthropic
    _claude = anthropic.Anthropic(api_key=_API_KEY, timeout=45.0, max_retries=1) if _API_KEY else None
    if not _API_KEY:
        _PREVIEW_OFF_WHY = "no API key in the environment (expected ANTHROPIC_API_KEY)"
except ImportError as e:
    anthropic = None
    _claude = None
    _PREVIEW_OFF_WHY = f"anthropic package not installed ({e})"

PREVIEW_SYSTEM = """You are the fridge-photo preview for Vasora AI, a nutrition app for people with \
type 1 or type 2 diabetes, high blood pressure, or weight and body fat goals.

You get one photo of someone's fridge (or pantry or groceries) and their answers to a short survey. \
Look at what is actually in the photo, then give short, practical food guidance for their conditions \
and goal, built from what they already have.

Rules:
- Only name foods you can actually see. If the photo isn't food storage or is too unclear to read, \
say so in the summary and keep the lists general.
- This is nutrition guidance, not medical advice. Never give insulin, medication or dosing advice, and \
never diagnose. For type 1 diabetes, mention carbs where useful but leave insulin to their care team.
- Respect every allergy, avoided food and preference in their notes. Never suggest something they said \
to avoid.
- If their BMI is under 18.5, or their notes mention an eating disorder, give no weight-loss advice and \
gently suggest talking to their doctor.
- Recipes should use mostly what is in the photo and fit the cuisines they like.
- Keep every item short: a few words for the name, one plain sentence for the reason. Write for a \
general reader, no jargon."""

_ITEM = {
    "type": "object",
    "properties": {"name": {"type": "string"}, "why": {"type": "string"}},
    "required": ["name", "why"],
    "additionalProperties": False,
}
READOUT_SCHEMA = {
    "type": "object",
    "properties": {
        "seen": {"type": "array", "items": {"type": "string"}},
        "summary": {"type": "string"},
        "cut": {"type": "array", "items": _ITEM},
        "add": {"type": "array", "items": _ITEM},
        "recipes": {"type": "array", "items": _ITEM},
    },
    "required": ["seen", "summary", "cut", "add", "recipes"],
    "additionalProperties": False,
}


def _client_ip(handler):
    fwd = handler.headers.get("X-Forwarded-For", "")
    return (fwd.split(",")[0].strip() if fwd else "") or handler.client_address[0]


def _preview_keys(ip, vid):
    key = (LIVE_TOKEN or "vasora-preview").encode()
    keys = ["ip:" + hmac.new(key, ip.encode(), hashlib.sha256).hexdigest()[:32]]
    if vid:
        keys.append("vid:" + hmac.new(key, vid.encode(), hashlib.sha256).hexdigest()[:32])
    return keys


def _claim_preview(keys):
    """Mark these keys as having used their free readout. False if any already had."""
    claimed = []
    for k in keys:
        if _kv:
            try:
                fresh = int(_kv.call("SADD", PREVIEW_USED_KEY, k) or 0) == 1
            except Exception as e:
                print(f"KV write failed, using memory: {e}", flush=True)
                fresh = None
        else:
            fresh = None
        if fresh is None:
            with _lock:
                fresh = k not in _preview_used_mem
                if fresh and len(_preview_used_mem) < MAX_MEM:
                    _preview_used_mem.add(k)
        if not fresh:
            _release_preview(claimed)
            return False
        claimed.append(k)
    return True


def _release_preview(keys):
    """Give the free readout back (the analysis failed)."""
    for k in keys:
        if _kv:
            try:
                _kv.call("SREM", PREVIEW_USED_KEY, k)
                continue
            except Exception:
                pass
        with _lock:
            _preview_used_mem.discard(k)


def _under_daily_cap():
    day = "preview:count:" + _today()
    if _kv:
        try:
            n = int(_kv.call("INCR", day) or 0)
            if n == 1:
                _kv.call("EXPIRE", day, 3 * 24 * 3600)
            return n <= PREVIEW_DAILY_CAP
        except Exception as e:
            print(f"KV write failed, using memory: {e}", flush=True)
    with _lock:
        _preview_day_mem[day] = _preview_day_mem.get(day, 0) + 1
        return _preview_day_mem[day] <= PREVIEW_DAILY_CAP


def _clean_profile(p):
    p = p if isinstance(p, dict) else {}
    text = lambda v, n: v.strip()[:n] if isinstance(v, str) else ""
    conds = [c for c in (p.get("conditions") or []) if c in CONDITIONS] if isinstance(p.get("conditions"), list) else []
    goal = p.get("goal") if p.get("goal") in GOALS else ""
    bmi = p.get("bmi")
    bmi = round(float(bmi), 1) if isinstance(bmi, (int, float)) and 8 <= bmi <= 120 else None
    return {
        "conditions": conds,
        "goal": goal,
        "bmi": bmi,
        "race_or_ethnicity": text(p.get("race"), 60),
        "foods_they_like": text(p.get("food"), 200),
        "notes": text(p.get("needs"), 600),
    }


def _analyze(image_b64, media_type, profile):
    """Ask Claude for a readout. Returns (status, payload)."""
    answers = json.dumps(profile, indent=1)
    try:
        response = _claude.messages.create(
            model=PREVIEW_MODEL,
            max_tokens=4000,
            system=PREVIEW_SYSTEM,
            output_config={"effort": "low", "format": {"type": "json_schema", "schema": READOUT_SCHEMA}},
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_b64}},
                    {"type": "text", "text": "My survey answers:\n" + answers +
                     "\n\nGive me up to 4 items to cut down on, up to 4 to add more of, and 2 to 3 recipes."},
                ],
            }],
        )
    except anthropic.BadRequestError as e:
        print(f"Claude rejected the request: {e.message}", flush=True)
        return 400, {"error": "bad_image"}
    except anthropic.RateLimitError:
        return 503, {"error": "busy"}
    except anthropic.APIStatusError as e:
        print(f"Claude API error {e.status_code}: {e.message}", flush=True)
        return 502, {"error": "ai_error"}
    except anthropic.APIConnectionError as e:
        print(f"Claude unreachable: {e}", flush=True)
        return 502, {"error": "ai_error"}
    if response.stop_reason == "refusal":
        return 422, {"error": "declined"}
    text = next((b.text for b in response.content if b.type == "text"), "")
    try:
        readout = json.loads(text)
    except ValueError:
        return 502, {"error": "ai_error"}
    for k in ("cut", "add", "recipes"):
        readout[k] = readout.get(k, [])[:5]
    readout["seen"] = readout.get("seen", [])[:20]
    return 200, {"readout": readout, "model": PREVIEW_MODEL}


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
    if not isinstance(token, str) or not token:
        return False
    ok = False
    for t in [LIVE_TOKEN] + VIEWER_TOKENS:  # check all, so timing doesn't reveal which matched
        if t and hmac.compare_digest(token.encode("utf-8"), t.encode("utf-8")):
            ok = True
    return ok


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
            stats["waitlist"] = _waitlist_count()
            stats["pending"] = _pending_count()
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
        route = self._route()
        if route not in ("/heartbeat", "/waitlist", "/queue", "/analyze"):
            self._send_json(404, {"error": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            length = -1
        limit = {"/queue": MAX_FORM_BODY, "/analyze": MAX_IMAGE_BODY}.get(route, MAX_BODY)
        if length < 0 or length > limit:
            self.close_connection = True
            self._send_json(413 if length > limit else 400, {"error": "bad_length"})
            return
        raw = self.rfile.read(length) if length else b""
        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, UnicodeDecodeError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        if route == "/analyze":
            self._handle_analyze(data)
            return
        if route == "/queue":
            fields = data.get("fields")
            if (not isinstance(fields, dict) or not fields or set(fields) - FORM_FIELDS
                    or not all(isinstance(v, str) for v in fields.values())):
                self._send_json(400, {"error": "bad_fields"})
                return
            email = fields.get("email", "")
            if not EMAIL_RE.match(email.strip()):
                self._send_json(400, {"error": "bad_email"})
                return
            if not _queue_form(fields):
                self._send_json(503, {"error": "busy"})
                return
            if fields.get("signup"):
                _record_signup(email)
            self._send_json(200, {"ok": True})
            return
        if route == "/waitlist":
            email = data.get("email")
            if not isinstance(email, str) or not EMAIL_RE.match(email.strip()):
                self._send_json(400, {"error": "bad_email"})
                return
            _record_signup(email)
            self._send_json(200, {"ok": True})
            return
        sid = data.get("sid")
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

    def _handle_analyze(self, data):
        if not _claude:
            self._send_json(503, {"error": "not_configured"})
            return
        image, media_type = data.get("image"), data.get("media_type")
        if not isinstance(image, str) or len(image) < 100 or media_type not in IMAGE_TYPES:
            self._send_json(400, {"error": "bad_image"})
            return
        unlimited = _token_ok(data.get("token"))
        keys = []
        if not unlimited:
            vid = data.get("vid")
            keys = _preview_keys(_client_ip(self), vid if isinstance(vid, str) and SID_RE.match(vid) else "")
            if not _claim_preview(keys):
                self._send_json(429, {"error": "used"})
                return
            if not _under_daily_cap():
                _release_preview(keys)
                self._send_json(503, {"error": "busy"})
                return
        status, payload = _analyze(image, media_type, _clean_profile(data.get("profile")))
        if status != 200:
            _release_preview(keys)
        payload["unlimited"] = unlimited
        self._send_json(status, payload)


def main():
    if not LIVE_TOKEN:
        print("WARNING: LIVE_TOKEN is not set - /count will always return 403.", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Vasora site + live counter listening on 0.0.0.0:{PORT}", flush=True)
    print("Visitor totals stored in " + ("Key Value store" if _kv else "memory (set KV_URL to persist)"), flush=True)
    print("AI preview " + ("on (" + PREVIEW_MODEL + ")" if _claude else "off: " + _PREVIEW_OFF_WHY), flush=True)
    threading.Thread(target=_backfill, daemon=True).start()
    threading.Thread(target=_retry_loop, daemon=True).start()
    server.serve_forever()


if __name__ == "__main__":
    main()
