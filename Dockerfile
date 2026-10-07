FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Manila
ENV DATA_DIR=/data
ENV HOME=/tmp

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.lock.txt .

RUN pip install --no-cache-dir -r requirements.lock.txt

RUN groupadd --gid 10001 district4 \
    && useradd --uid 10001 --gid district4 --no-create-home district4 \
    && mkdir /data \
    && chown district4:district4 /data

# Copy only runtime code/assets. Local secrets and databases never enter layers.
COPY *.py ./
COPY templates/ ./templates/
COPY static/ ./static/

USER district4

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3)" || exit 1

CMD ["gunicorn", "--config", "gunicorn.conf.py", "app:app"]
