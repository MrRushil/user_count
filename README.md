# Vasora AI website

The Vasora AI landing page and its owner-only live viewer counter, served by one
dependency-free Python process (`server.py`).

- `public/index.html` - the landing page
- `public/frames/hero/` - the 336 hero animation frames
- `server.py` - serves `public/` plus `POST /heartbeat`, `GET /count?token=...`, `GET /health`

## Run locally

    PORT=8000 LIVE_TOKEN=your-secret python3 server.py

Open http://localhost:8000 (add `?live=your-secret` to see the viewer badge).

## Render

- Build command: `pip install -r requirements.txt` (nothing to install; the file is empty)
- Start command: `python3 server.py`
- Environment variable: `LIVE_TOKEN` = the owner token
