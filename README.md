# Retreivr

<p align="center">
  <img src="webUI/app_icon.png" width="128" />
</p>

<h1 align="center">Retreivr</h1>

<p align="center">
  Self-hosted media search, scoring, and archival engine
</p>

## Overview
Retreivr is an Open-Source Media Search and Download tool designed to streamline the user's search for freely-available media content and give them the power to quickly find new content for their self-hosted Media Library, while staying privacy-focused, fully in control, and easy to use. 

My vision for this tool is not to replace any Library-Management software or Media-Players, but to be the premier application for searching for new content to collect. First-stop for new content on any self-hosted media library.

Already this project boasts a clean Web UI, a simple API, and a Docker-first deployment model. 
For most users, the recommended install and best experience is the Docker container.

## History
Retreivr is the evolution of the former YouTube-Archiver project.
The project has been renamed and repositioned to reflect its broader role as a controlled media discovery and library seeding system.

Version v0.9.0 is the first release under the Retreivr name.

## Functionality
It’s designed to run unattended, keep history in a local SQLite database, and let you inspect status, logs, and completed downloads from a browser. Files are downloaded to disk once and can be downloaded to the server or downloaded directly to your device via the Web UI.

This is not a cloud service and it does not require any hosted components.

What this tool does well
	•	Keeping personal or shared YouTube playlists in sync
	•	Running scheduled archive jobs without cron or babysitting
	•	Downloading a single URL on demand
	•	Reviewing status, progress, logs, and history from a browser
	•	Downloading completed files directly from the server
	•	Running cleanly in Docker with explicit, safe volume mappings

Highlights
	•	Mobile-friendly Web UI served by the API
	•	Built-in scheduler (no cron, no systemd)
	•	Docker-safe path handling and volume layout
	•	Background runs with live playlist + video progress
	•	SQLite history with search, filter, and sort
	•	Optional Telegram run summaries
	•	Manual yt-dlp update button (restart required)
	•	Optional Basic auth and reverse-proxy support
	•	Home Assistant–friendly status and metrics endpoints

## Screenshots

### Home Page
![Home Page](webUI/assets/screenshots/screenshot_1.png)

## Quick start (Docker - recommended)
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

OR clone from Github:

## Quick Start

```bash
git clone https://github.com/Retreivr/retreivr.git
cd retreivr
cp docker-compose.yml.example docker-compose.yml
docker compose up -d
```

## Requirements
Docker deployment:
- Docker Engine or Docker Desktop
- docker compose (v2)

Local/source deployment (optional):
- Python 3.11 only
- ffmpeg on PATH
- Node.js or Deno only if you use a JS runtime for extractor workarounds

## Configuration
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

## Watcher (optional)
The watcher polls playlists using the YouTube Data API and adapts its interval based on activity. Detections are batched using a quiet-window strategy (60 seconds of no new detections), then the watcher runs all queued playlists back-to-back. When downtime is active, polling pauses but queued downloads can still run.

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
When `music_mode` is enabled and `music_metadata.enabled` is true, the app enqueues the finalized file for background enrichment using MusicBrainz (optional AcoustID). This runs asynchronously and does not block downloads. Files are never renamed, and existing rich tags are not overwritten.

Example config:
```json
"music_metadata": {
  "enabled": true,
  "confidence_threshold": 70,
  "use_acoustid": false,
  "acoustid_api_key": "",
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

## Subscribe mode (optional)
Subscribe mode is opt-in per playlist and only downloads new videos after the first run.

Behavior:
- First run: records all current video IDs as seen and downloads nothing.
- Subsequent runs: downloads only videos not already seen for that playlist.

Mode interaction:
- subscribe + music_mode → allowed
- subscribe + video → allowed
- full + music_mode → allowed
Subscribe logic is orthogonal to media type.

## Path strategy (Docker)
The app always writes to `/downloads`. You control where that maps on your system.
Example mapping to `/Media` on the host:
```
- /Media:/downloads
```
Example playlist folder values (relative to `/downloads` inside the container):
- `Videos/ChannelName`
- `Music/Artist/Album`

Do not put absolute host paths in `config.json` when using Docker.

## Web UI
The Web UI is served by the API and talks only to REST endpoints. It provides:
	•	Home page with run controls, status, schedule, and metrics
	•	Config page (including schedule controls and optional playlist names)
	•	OAuth helper to generate tokens directly from the Config page
	•	Downloads page with search and limit controls
	•	History page with search, filter, sort, and limit controls
	•	Logs page with manual refresh
	•	Live playlist progress + per-video download progress
	•	Current phase and last error in Status
	•	App version + update availability (GitHub release check)
	•	Download buttons for completed files
	•	Single-URL delivery mode (server library or one-time client download)
	•	Manual cleanup for temporary files
	•	Manual yt-dlp update button (restart container after update)
	•	Single-playlist runs on demand (without editing config)
	•	Kill downloads in progress (cancel active run)

## API overview
Common endpoints:
	•	GET /api/status
	•	GET /api/metrics
	•	GET /api/schedule
	•	POST /api/run
	•	GET /api/history
	•	GET /api/logs


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
	•	For group chats, add the bot to the group and send a message first.
	•	Group chat IDs are usually negative numbers.

## Updating
Containers are disposable; your real data lives in mounted volumes. A safe update flow is:
```bash
docker compose pull
docker compose down
docker compose up -d
```
This preserves your config, database, logs, tokens, and downloads.

## Versioning (Docker builds)
The app reads its version from `YT_ARCHIVER_VERSION`. The Dockerfile exposes a build arg:
```bash
docker build -f docker/Dockerfile --build-arg YT_ARCHIVER_VERSION=0.9.0 -t retreivr:latest .
```
This avoids keeping the version in Compose or runtime envs.

## Security model
	•	Local-first design; no hosted or cloud mode
	•	Optional Basic auth (off by default)
	•	Reverse-proxy friendly (X-Forwarded-* headers supported)
	•	No secrets exposed to frontend JavaScript
	•	OAuth tokens are stored locally and transparently under TOKENS_DIR

## What this tool does not attempt to do
This project does not attempt to:
	•	Circumvent DRM
	•	Auto-update yt-dlp at runtime
	•	Act as a hosted or cloud service
	•	Collect telemetry or usage data
	•	Bypass platform terms of service
	•	Provide real-time detection (playlist checks are scheduled/polled)
	•	Run with multiple API workers (single-worker design is required for the watcher)
	•	Guarantee complete music metadata (fields may be missing depending on source and cookies)

## Notes
	•	Downloads are staged in a temp directory and atomically copied to their final location
	•	“Clear temporary files” only removes working directories (temp downloads + yt-dlp temp)
	•	“Update yt-dlp” runs in-container and requires a container restart to take effect
	•	YT_ARCHIVER_* environment variables can override paths (see .env.example)

## Download execution model
Downloads use a two-phase model:
	•	Native (v1.2.0-equivalent semantics) first: a single yt-dlp invocation with explicit Node.js runtime and remote JS solver enabled to expose muxed video formats.
	•	Hardened fallback (v1.3.0): only used if native fails, with the existing client forcing, retry ladder, and safety guards.
Native success requires a muxed video output. Audio-only outputs are rejected and trigger fallback.
Guiding principle: “If yt-dlp CLI succeeds with no flags, the app should succeed with no retries.”

## Release
See `CHANGELOG.md` for details of the current release and history.

## Contributing
Contributions are welcome. Please read `CONTRIBUTING.md` before opening a PR.

## Security
Security issues should be reported privately. See `SECURITY.md`.

## License
MIT. See `LICENSE`.
