# ---------------------------------------------------------------------------
# Mutual Fund Performance Trend Analytics - Kafka + HDFS + MapReduce dashboard
# ---------------------------------------------------------------------------
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=5050

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install -r requirements.txt

# Application code + data
COPY . .

# Run as an unprivileged user that owns the writable data-lake / state folders
RUN sed -i 's/\r$//' docker/entrypoint.sh \
    && chmod +x docker/entrypoint.sh \
    && useradd --create-home --uid 1000 bda \
    && chown -R bda:bda /app
USER bda

EXPOSE 5050

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.environ.get('PORT','5050'), timeout=3)"

ENTRYPOINT ["docker/entrypoint.sh"]
