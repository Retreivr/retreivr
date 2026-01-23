# Changelog

All notable changes to this project will be documented here.

## [v0.9.2] – Search Engine Dialed In // Home Page UI Update

Highlights

This release hardens the download pipeline (especially audio-only MP3), improves observability, and simplifies the Home UI ahead of broader feature work. Video downloads remain stable and unchanged.

⸻

🚀 Improvements
	•	Reliable MP3 audio-only downloads
	•	Audio mode now uses a robust bestaudio[acodec!=none]/bestaudio/best selector.
	•	Prevents unnecessary video downloads when targeting MP3.
	•	Matches known-working yt-dlp CLI behavior.
	•	Works consistently for direct URLs and queued jobs.
	•	Safer yt-dlp option handling
	•	Avoids forced merge/remux unless explicitly required.
	•	Reduces ffmpeg post-processing failures.
	•	Audio and video paths are now clearly separated and predictable.
	•	yt-dlp CLI observability
	•	Job workers now log the exact yt-dlp CLI command executed (with secrets redacted).
	•	Makes debugging format, cookie, and extractor issues significantly easier.

⸻

🧠 Behavior Fixes
	•	Post-processing failures are now terminal
	•	ffmpeg / post-processing errors correctly mark jobs as FAILED.
	•	Prevents silent re-queue loops and misleading “Queued” states in the UI.
	•	Video pipeline preserved
	•	Default video behavior (bestvideo+bestaudio/best) remains unchanged.
	•	MP4 / MKV / WebM downloads continue to work as before.

⸻

🎧 Music & Metadata
	•	Music metadata enrichment remains optional
	•	Failed or low-confidence enrichment no longer blocks successful downloads.
	•	Clear logging when metadata is skipped due to confidence thresholds.

⸻

🖥 UI / UX
	•	Home page cleanup
	•	Reorganized source filters and advanced options into a single compact row.
	•	Reduced visual noise without removing functionality.
	•	Improved spacing and alignment for music mode, format, and destination controls.
	•	Advanced Search remains available
	•	Advanced functionality is still accessible via the dedicated Advanced Search page.

⸻

🧹 Internal / Maintenance
	•	Improved internal option auditing logs.
	•	Better separation between search, enqueue, and execution logic.
	•	No schema or config migrations required.

⸻

⚠️ Known Notes
	•	Client-side (“download to this device”) delivery is still being refined and may be disabled or hidden in some UI paths.
  

## [v0.9.1] – Runtime Stability & Direct URL Fixes

This release focuses on restoring and hardening runtime stability after refactors since yt-archiver v1.2.0.
Primary goals were correctness, predictability, and eliminating regressions in downloads, scheduling, and search flows.

Fixed:
- Restored reliable Direct URL downloads for video and audio (mp3/m4a/etc).
- Corrected yt-dlp invocation for audio formats (uses extract-audio instead of merge-output-format).
- Fixed Direct URL runs appearing permanently queued in the Home UI.
- Prevented empty or zero-byte output files from being recorded as completed.
- Fixed scheduler playlist downloads producing incorrect formats or audio-only output.
- Ensured scheduler and direct downloads can run concurrently without interference.
- Fixed missing database schema initialization for search-related tables.
- Normalized all filesystem paths via paths.py and environment variables (Docker-safe).
- Fixed Advanced Search “Failed to load requests” error caused by search DB store calling service-only logic.
- Fixed Home screen results remaining stuck in “Queued” by restoring reliable search request status hydration.
- Unified search job database usage to a single canonical path to prevent schema and state mismatches.

Changed:
- Direct URL playlist links are now explicitly rejected with a clear user-facing error message.
- Direct URL runs bypass the job queue but still report progress and completion via run status.
- Search-only results can now be downloaded individually via the Home results UI.
- Default video downloads respect configured format preferences (e.g., webm/mp4).
- Metadata enrichment failures no longer block or corrupt completed downloads.

Notes:
- Playlist URLs must be added via Scheduler / Playlist configuration, not Direct URL mode.
- Kill-download button is not guaranteed during active runs and remains experimental.
- Watcher functionality is present but considered beta and may change in later releases.

## [v0.9.0] – Retreivr Rebrand Release // Music Mode and Metadata
- Project renamed to Retreivr
- Repository migrated to new namespace
- Branding updated across documentation, UI, and Docker artifacts

Added:
- Music mode (opt-in per playlist and per single-URL run) with audio-focused metadata and music-safe naming.
- Smart Search feature - aggregating results from multiple top-tier sources and scoring quality and metadata acquisition, ranking results, and simplifying the entire media-searching process for users.
- yt-dlp cookies support (Netscape cookies.txt) for improved YouTube Music metadata.
- Music filename template support (artist/album/track hierarchy).
- Music metadata enrichment pipeline (MusicBrainz + optional AcoustID + artwork) with background tagging.
- UI controls for music mode, cookies, music template, and metadata enrichment options.
- Config UI controls for watcher enable, backoff timing, and downtime window.
- Per-playlist subscribe mode (only download new videos after first run).
- Single-playlist run controls in the Web UI.
- Single-URL delivery modes (server library or one-time client download via HTTP).
- Button to Kill downloads in progress (cancel active run from Status Section).
- Adaptive watcher with per-playlist state persisted in SQLite.
- Watch policy config with downtime windows and backoff timing.

Changed:
- Metadata flow now prefers yt-dlp music fields when music mode is enabled.
- Music metadata tagging can overwrite existing yt-dlp tags by default (configurable).
- Music URLs (music.youtube.com) auto-enable music mode for single URL runs.
- Music mode download URLs use music.youtube.com when enabled.
- Music mode respects video formats (does not force audio when final_format is a video).
- Download execution uses a native single-call path with explicit JS runtime/solver and a muxed-video requirement, with hardened fallback on failure.
- Watcher uses a single supervisor loop and deterministic scheduling.
- Downloads respect downtime windows and defer until downtime ends.
- Watcher batches detections with a quiet-window strategy and sends one Telegram summary per batch.
- Status now reports current phase and last error for active runs.

### Summary
This v0.9.0 release marks the official rebrand of the project from **YouTube-Archiver** to **Retreivr**.
The version number has been reset to reflect a new product identity and roadmap.

### Important Notes
- v1.2.0 was the **final release** under the YouTube-Archiver name.
- Versioning resumes at v0.9.0 under the Retreivr name.
- Functionality is unchanged in this release.
- Future releases may introduce breaking changes as APIs, environment variables, and UI contracts are stabilized.

|vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv|
|##################################|
|//////////////////////////////////|
|\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\|
|##################################|
|^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^|

## [v1.2.0] – Final YouTube-Archiver Release

### Summary
Final release published under the YouTube-Archiver name before rebranding.

### Changes

1.2.0 - OAuth Web UI Helper
Added:
- Web UI OAuth helper per account (launches Google auth URL + paste code to save token).
- API endpoints to support OAuth in the Web UI flow.

Notes:
- Docker pulls remain available via GHCR:
- `docker pull ghcr.io/retreivr/retreivr:latest`
- `image: ghcr.io/retreivr/retreivr:latest`

1.1.2 - Publish Docker to GHCR Latest
No changes from v1.1.1 - just creating new tag to automatically publish to GHCR repo to make docker pulls directly from there.

Docker Pull Command:
docker pull ghcr.io/retreivr/retreivr:latest

Docker Compose Line:
image: ghcr.io/retreivr/retreivr:latest

1.1.1 - Minor Patch
Added:
- prompts to verify you wish to remove items on 'Remove' button press

Changed:
- hardened version control by clearing caches.
- better date/time format in Status block.
- clearer log entries when Google Oauth runs to verify playlists.

1.1.0 - Version Control and YT-DLP Update Maintenance
Added:
- App version display and GitHub release check in Status.
- Manual yt-dlp update button (requires restart).

Changed:
- Dockerfile build arg for `YT_ARCHIVER_VERSION`.

1.0.1 - Frontend UI Updates
Added:
- Multi-page Web UI (Home, Config, Downloads, History, Logs) with top navigation.
- Separate playlist progress vs per-video download progress indicators.
- Downloads + History filters with limits, and internal scrolling tables.
- Mobile navigation menu + collapsible filters for small screens.
- Telegram summary truncation to avoid oversized messages.
- Playlist name field in config editor (non-functional label).

Changed:
- Default Downloads/History limit to 50.
- Light-mode header styling and mobile spacing tweaks.

Fixed:
- History “Copy URL” now copies the YouTube URL, not API download URL.
- Config/playlist download paths normalized to remain relative to /downloads.

1.0.0 - First public release
Added:
- Docker-first deployment with explicit volume paths.
- FastAPI backend serving API + static Web UI.
- Web UI for config, runs, status, logs, history, downloads, and cleanup.
- Built-in scheduler (no cron, no systemd).
- SQLite-backed history and state.
- Optional Telegram summaries and optional Basic auth.
- Home Assistant-friendly status and metrics endpoints via API.
- desktop GUI deprecated

Prior to 1.0.0
- no official release
- only python scripts released, no official package
- archiver.py ran independently
- a desktop GUI was created, paving way for the eventual webUI
