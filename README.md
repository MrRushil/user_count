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

## AI preview (Claude Haiku)

`POST /analyze` sends a visitor's fridge photo (shrunk to ~1024px in the browser) and survey answers to Claude Haiku (`claude-haiku-5-5`) and returns a readout. Photos are not stored.

- One free readout per person, tracked by a keyed hash of their IP and of a per-browser id; a failed analysis doesn't use it up.
- Anyone opening the site with a viewer link (`?live=` with `LIVE_TOKEN` or one of `VIEWER_TOKENS`) gets unlimited readouts and the live badge.
- `PREVIEW_DAILY_CAP` (default 300) caps free readouts per day across everyone.
- Needs `ANTHROPIC_API_KEY` in the Render environment and the build command `pip install -r requirements.txt`. Without them the page falls back to its built-in sample readout.

