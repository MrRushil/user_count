# Vasora AI website

The Vasora AI landing page and its owner-only live viewer counter, served by one
dependency-free Python process (`server.py`).

- `public/index.html` - the landing page
- `public/frames/hero/` - the hero animation: 42 sprite sheets (8 frames each) plus the first frame
- `server.py` - serves `public/` plus `POST /heartbeat`, `GET /count?token=...` (live viewers, distinct visitors today / this week / all time, and waitlist size); `POST /waitlist` counts signups by a keyed hash of the email, `POST /queue` holds a form when FormSubmit is down and retries delivery every 2 minutes until it goes through, `GET /health`

## Run locally

    PORT=8000 LIVE_TOKEN=your-secret python3 server.py

Open http://localhost:8000 (add `?live=your-secret` to see the viewer badge).

## Render

- Build command: `pip install -r requirements.txt` (nothing to install; the file is empty)
- Start command: `python3 server.py`
- Environment variables: `LIVE_TOKEN` = the owner token; `KV_URL` = the internal URL of the `vasora-visitors` Key Value store (keeps the daily total when the free service sleeps); `DAY_TZ` = time zone for "today" (default America/Los_Angeles)
