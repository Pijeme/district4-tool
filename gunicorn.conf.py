"""One process keeps the app's in-memory background job state consistent."""

import os

bind = "0.0.0.0:8000"
workers = 1
threads = int(os.getenv("GUNICORN_THREADS", "4"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "180"))
graceful_timeout = 30
accesslog = "-"
errorlog = "-"
