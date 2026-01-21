# Retreivr — Portainer Deployment (Docker Standalone) — v0.9.1

This guide shows how to deploy Retreivr using Portainer Stacks, using the already-published image.
Example portainer-friendly docker-compose.yml file is located in /docker folder.

## 1) Create host folders (persistent storage)

Choose a host folder (example: `/srv/retreivr`) and create:

- `/srv/retreivr/downloads` (media output)
- `/srv/retreivr/data` (app data, DB/temp)
- `/srv/retreivr/config` (Retreivr config.json)
- `/srv/retreivr/logs`
- `/srv/retreivr/tokens` (OAuth tokens, if/when used)

## 2) Create a Stack in Portainer

Portainer:
- Stacks → Add stack
- Name: `retreivr`
- Method: Web editor

Paste one of the compose files below and Deploy.

## Option A: Docker Hub (pinned)

```yaml
version: "3.9"

services:
  retreivr:
    image: retreivr/retreivr:0.9.1
    container_name: retreivr
    ports:
      - "8090:8000"
    environment:
      - RETREIVR_DOWNLOADS_DIR=/downloads
      - RETREIVR_DATA_DIR=/data
      - RETREIVR_LOG_DIR=/logs
      - RETREIVR_TOKENS_DIR=/tokens
      - RETREIVR_HOST=0.0.0.0
      - RETREIVR_PORT=8000
    volumes:
      - /srv/retreivr/downloads:/downloads
      - /srv/retreivr/data:/data
      - /srv/retreivr/config:/config
      - /srv/retreivr/logs:/logs
      - /srv/retreivr/tokens:/tokens
    restart: unless-stopped
```

## Option B: GHCR (pinned)
```yaml
version: "3.9"

services:
  retreivr:
    image: ghcr.io/retreivr/retreivr:0.9.1
    container_name: retreivr
    ports:
      - "8090:8000"
    environment:
      - RETREIVR_DOWNLOADS_DIR=/downloads
      - RETREIVR_DATA_DIR=/data
      - RETREIVR_LOG_DIR=/logs
      - RETREIVR_TOKENS_DIR=/tokens
      - RETREIVR_HOST=0.0.0.0
      - RETREIVR_PORT=8000
    volumes:
      - /srv/retreivr/downloads:/downloads
      - /srv/retreivr/data:/data
      - /srv/retreivr/config:/config
      - /srv/retreivr/logs:/logs
      - /srv/retreivr/tokens:/tokens
    restart: unless-stopped
```

## 3) Configuration file

Place your config file at:
	•	/srv/retreivr/config/config.json

The container mounts this folder to /config.

## 4) Open the UI/API

Default URL:
	•	http://<host-ip>:8090

Notes
	•	Portainer deployments should prefer absolute host paths. Avoid ./downloads style binds in Portainer stacks.
	•	Playlist folder paths in config.json should be relative to /downloads inside the container.

---
