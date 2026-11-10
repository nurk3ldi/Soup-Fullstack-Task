FROM python:3.12-slim AS app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
EXPOSE 8000
# --no-access-log: uvicorn's access log prints full URLs; the app logs requests itself, redacted.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--timeout-graceful-shutdown", "3"]

# Test image: same code plus pytest and headless Chromium. Kept separate so the app image stays small.
FROM app AS tests
COPY requirements-test.txt .
# libatomic1: Playwright's bundled node binary needs it and python:3.12-slim does not ship it.
# Separate layers so a stalled download only repeats its own step.
RUN apt-get update && apt-get install -y --no-install-recommends libatomic1 \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --timeout 30 --retries 5 -r requirements-test.txt
RUN playwright install --with-deps chromium && rm -rf /var/lib/apt/lists/*
COPY pytest.ini .
COPY tests ./tests
COPY loadtest ./loadtest
CMD ["pytest", "-v"]
