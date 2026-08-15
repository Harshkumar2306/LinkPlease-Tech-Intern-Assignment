FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# SQLite lives on a volume so queued DMs survive restarts/restarts.
ENV DATABASE_PATH=/data/linkplease.db
VOLUME /data

EXPOSE 8000

# Exactly one worker: the dedup claim, rate limiter, and single sender worker
# assume a single process. Do NOT raise the worker count.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
