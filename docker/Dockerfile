FROM python:3.11-slim

ARG YT_ARCHIVER_VERSION=1.3.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    YT_ARCHIVER_VERSION=${YT_ARCHIVER_VERSION} \
    YT_ARCHIVER_CONFIG_DIR=/config \
    YT_ARCHIVER_DATA_DIR=/data \
    YT_ARCHIVER_DOWNLOADS_DIR=/downloads \
    YT_ARCHIVER_LOG_DIR=/logs \
    YT_ARCHIVER_TOKENS_DIR=/tokens

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        tzdata \
        nodejs \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
    && pip install --no-cache-dir yt-dlp

COPY . /app

RUN mkdir -p /downloads /data /config /logs /tokens 

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
