# Retreivr

<p align="center">
  <img src="webUI/app_icon.png" width="128" />
</p>

<p align="center">
  Powerful, self-hosted media search, archival, and metadata imbedding engine
</p>

## Overview
Retreivr is a self-hosted media search and archival engine focused on discovering, scoring, and archiving publicly available media.  
It provides an advanced search pipeline, a unified FIFO download queue, and post-download metadata enrichment, with an initial focus on music. 

## History
Retreivr is the successor to the YouTube-Archiver project.  
Version v0.9.x represents the first stable pre-1.0 release series under the Retreivr name.

## Functionality
Retreivr runs as a local service backed by SQLite, exposing a Web UI and API for search, queue inspection, logs, and completed downloads.  
All downloads are processed exactly once through a unified worker queue and written to disk.

## Core Capabilities
- Keeping personal or shared YouTube playlists in sync  
- Running scheduled archive jobs without cron or babysitting  
- Downloading a single URL on demand  
- Reviewing status, progress, logs, and history from a browser  
- Downloading completed files directly from the server  
- Running cleanly in Docker with explicit, safe volume mappings  
- Mobile-friendly Web UI served by the API  
- Built-in scheduler (no cron, no systemd)  
- Docker-safe path handling and volume layout  
- Background runs with live playlist + video progress  
- SQLite history with search, filter, and sort  
- Manual yt-dlp update button (restart required)  
- Optional Basic auth and reverse-proxy support  
- Download buttons for completed files  
- Single-URL delivery mode (server library or one-time client download)  
- Manual cleanup for temporary files  
- Single-playlist runs on demand (without editing config)  
- Current phase and last error in Status  
- App version + update availability (GitHub release check)  

## Web UI Screenshots

### Home Page
![Home Page](webUI/assets/screenshots/screenshot_1.png)

## Quick Start (Docker - recommended)
Pull the prebuilt image from GHCR:
```bash
docker pull ghcr.io/retreivr/retreivr:latest
```
The image is published under GitHub Packages for this repo.

Copy the Docker and env templates, then start:
```bash
cp docker/docker-compose.yml.example docker/docker-compose.yml
cp .env.example .env
docker compose -f docker/docker-compose.yml up -d
```
Open the Web UI at `http://YOUR_HOST:8090`.

## Quick Start (Local/source)
```bash
git clone https://github.com/Retreivr/retreivr.git
cd retreivr
cp docker-compose.yml.example docker-compose.yml
docker compose up -d
```

Docker deployment is the recommended path for most users.

For Portainer deployment, see portainer.md and /docker/docker-compose.portainer.yml.example

## Requirements
Docker deployment:
- Docker Engine or Docker Desktop  
- docker compose (v2)  

Local/source deployment (optional):  
- Python 3.11 only  
- ffmpeg on PATH  
- Node.js or Deno only if you use a JS runtime for extractor workarounds  

## Configuration
Most users only need to edit `config/config.json` and set download paths.

1) Copy the sample config:
```bash
cp config/config_sample.json config/config.json
```

Config path usage:  
- Web UI / API runs use the server’s active config path (`/api/config/path`, default `config/config.json`).  
- CLI runs use the path passed to `scripts/archiver.py` (or its default if omitted).  

2) (OPTIONAL) Create a Google Cloud OAuth client (Type: Desktop app) and place client secret JSONs in `tokens/`.  

3) (OPTIONAL) Generate OAuth tokens:  
Web UI (recommended):  
- Config page → Accounts → fill Account, Client Secret, Token path  
- Click “Run OAuth”, open the URL, approve, then paste the code to save the token  

CLI fallback:  
```bash
python scripts/setup_oauth.py --account family_tv tokens/client_secret_family.json tokens/token_family.json
```

4) Edit `config/config.json`:  
- `accounts` paths to client_secret and token JSONs (optional if you only use public playlists)  
- `playlists` with `playlist_id`, `folder`, optional `account`, optional `final_format`, optional `music_mode`, optional `mode` (full/subscribe)  
- `final_format` default (webm/mp4/mkv/mp3)  
- `music_filename_template` optional music-safe naming (artist/album/track)  
- `yt_dlp_cookies` optional Netscape cookies.txt for improved music metadata  
- `js_runtime` to avoid extractor issues (node:/path or deno:/path)  
- `single_download_folder` default for single-URL downloads  
- `telegram` optional bot_token/chat_id for summaries (see Telegram setup below)  
- `schedule` optional interval scheduler  
- `watch_policy` optional adaptive watcher with downtime window (local time)  

## Music mode (optional)
Music mode is opt-in per playlist and per single-URL run. It applies music-focused metadata and uses yt-dlp music metadata when available. When enabled, download URLs use `music.youtube.com`.

Recommendations:  
- Provide a Netscape `cookies.txt` file via `yt_dlp_cookies` (stored under `tokens/`) for the best YouTube Music metadata.  
- Use a music filename template such as:  
  `%(artist)s/%(album)s/%(track_number)s - %(track)s.%(ext)s`  

Notes:  
- If cookies are missing, music metadata quality may be degraded.  
- Single-URL runs auto-enable music mode when the URL is `music.youtube.com`.  
- If `final_format` is a video format (webm/mp4/mkv), the download remains video even in music mode. Use an audio format (mp3/m4a/flac/opus) to force audio-only.

## Music metadata enrichment (optional)
When `music_mode` is enabled and `music_metadata.enabled` is true, the app enqueues the finalized file for background enrichment using Spotify as the canonical metadata source, with MusicBrainz as an optional fallback. This runs asynchronously and does not block downloads. Files are never renamed, and existing rich tags are not overwritten.

Example config:
```json
"music_metadata": {
  "enabled": true,
  "confidence_threshold": 70,
  "embed_artwork": true,
  "allow_overwrite_tags": true,
  "max_artwork_size_px": 1500,
  "rate_limit_seconds": 1.5,
  "dry_run": false
}
```

Tagged files preserve YouTube traceability via custom tags (SOURCE, SOURCE_TITLE, MBID when matched). By default, enriched tags overwrite existing yt-dlp tags; set `allow_overwrite_tags` to false to keep original tags intact.

## Single-URL delivery modes
Single-URL runs support an explicit delivery mode:  
- `server` (default): save into the server library (`single_download_folder`).  
- `client`: stage the finalized file for a one-time HTTP download to the browser, then delete it after transfer or timeout (~10 minutes).  

Delivery mode applies to single-URL runs only; playlists and watcher runs always save to the server library. Validation and conversion still occur before any delivery.

## Direct URL limitations
Direct URL mode is intentionally limited to **single media items only**.

- Playlist URLs are **not supported** in Direct URL mode.
- If a playlist URL is entered, the run will fail immediately with a clear error message.
- To archive playlists, use the **Scheduler** or **Playlist** configuration instead.

This design keeps Direct URL runs fast, predictable, and isolated from long-running playlist jobs.

## Web UI
The Web UI is served by the API and talks only to REST endpoints. It provides:  
- Home page with run controls, status, schedule, and metrics  
- Config page (including schedule controls and optional playlist names)  
- OAuth helper to generate tokens directly from the Config page  
- Downloads page with search and limit controls  
- History page with search, filter, sort, and limit controls  
- Logs page with manual refresh  
- Live playlist progress + per-video download progress  
- Current phase and last error in Status  
- App version + update availability (GitHub release check)  
- Download buttons for completed files  
- Single-URL delivery mode (server library or one-time client download)  
- Manual cleanup for temporary files  
- Manual yt-dlp update button (restart container after update)  
- Single-playlist runs on demand (without editing config)  

## API overview
Common endpoints:  
- GET /api/status  
- GET /api/metrics  
- GET /api/schedule  
- POST /api/run  
- GET /api/history  
- GET /api/logs  

OpenAPI docs are available at `/docs`.

## Telegram notifications (optional)
You must create your own bot and provide both the bot token and chat ID.

Quick setup:  
1) Talk to @BotFather in Telegram and create a bot to get the token.  
2) Start a chat with the new bot and send a message.  
3) Get your chat ID by visiting:  
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`  
   Look for `"chat":{"id":...}` in the response.  
4) Set these in `config.json`:  
```
"telegram": {
  "bot_token": "YOUR_BOT_TOKEN",
  "chat_id": "YOUR_CHAT_ID"
}
```

Notes:  
- For group chats, add the bot to the group and send a message first.  
- Group chat IDs are usually negative numbers.

## Documentation

- [Path & Volume Layout](docs/paths.md)
- [Portainer Setup](docs/portainer.md)

## Updating
Containers are disposable; your real data lives in mounted volumes. A safe update flow is:
```bash
docker compose pull
docker compose down
docker compose up -d
```
This preserves your config, database, logs, tokens, and downloads.

## Versioning (Docker builds)
The app reads its version from `RETREIVR_VERSION`. The Dockerfile exposes a build arg:
```bash
docker build -f docker/Dockerfile --build-arg RETRIEVR_VERSION=0.9.1 -t retreivr:latest .
```
This avoids keeping the version in Compose or runtime envs.

## Security
Retreivr is designed as a local-first application with no hosted or cloud mode. It supports optional Basic auth and is reverse-proxy friendly. OAuth tokens and sensitive data are stored locally and not exposed to frontend JavaScript.

## Project Scope (v0.9.x)
- Music-focused search and downloads  
- Public, non-DRM sources only  
- Single-worker, deterministic execution  
- UI and APIs are stable but evolving  
- Direct URL mode is restricted to single-item downloads; playlists must use scheduled or playlist runs.

## What this tool does not attempt to do
This project does not attempt to:  
- Circumvent DRM  
- Auto-update yt-dlp at runtime  
- Act as a hosted or cloud service  
- Collect telemetry or usage data  
- Bypass platform terms of service  
- Provide real-time detection (playlist checks are scheduled/polled)  
- Run with multiple API workers (single-worker design is required for the watcher)  
- Guarantee complete music metadata (fields may be missing depending on source and cookies)  

## Notes
- Downloads are staged in a temp directory and atomically copied to their final location  
- “Clear temporary files” only removes working directories (temp downloads + yt-dlp temp)  
- “Update yt-dlp” runs in-container and requires a container restart to take effect  
- RETREIVR_* environment variables can override paths (see .env.example)  

## Release
See `CHANGELOG.md` for details of the current release and history.

## Contributing
Contributions are welcome. Please read `CONTRIBUTING.md` before opening a PR.

## Security
Security issues should be reported privately. See `SECURITY.md`.

## License
MIT. See `LICENSE`.
