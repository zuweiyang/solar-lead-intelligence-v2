# OmniSol Tracking Service — Cloud Run Dockerfile
# Minimal image: only Flask + Gunicorn, no pipeline dependencies.

FROM python:3.11-slim

# Cloud Run best practice: don't run as root
RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

USER appuser

# Cloud Run injects PORT at runtime. Gunicorn binds to it.
# --workers 1: Cloud Run scales via instances, not workers.
# --threads 8: handle concurrent open/click pings within one instance.
# --timeout 0: Cloud Run handles request timeouts externally.
CMD exec gunicorn --bind "0.0.0.0:${PORT}" \
    --workers 1 \
    --threads 8 \
    --timeout 0 \
    main:app
