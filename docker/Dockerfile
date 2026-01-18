FROM python:3.11-slim

ARG RETREIVR_VERSION=0.9.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RETREIVR_VERSION=${RETREIVR_VERSION} \
    RETREIVR_CONFIG_DIR=/config \
    RETREIVR_DATA_DIR=/data \
    RETREIVR_DOWNLOADS_DIR=/downloads \
    RETREIVR_LOG_DIR=/logs \
    RETREIVR_TOKENS_DIR=/tokens

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
