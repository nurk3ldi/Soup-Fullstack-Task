FROM python:3.12-slim AS app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv
# Security updates the base image does not carry yet (zlib1g at the time of writing), and an
# unprivileged user to run as.
# hadolint ignore=DL3005
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
EXPOSE 8000
USER 10001:10001
# --no-access-log: uvicorn's access log prints full URLs; the app logs requests itself, redacted.
# --no-server-header: do not advertise the server software.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--no-server-header", "--timeout-graceful-shutdown", "3"]

# Test image: same code plus pytest and headless Chromium. Kept separate so the app image stays small.
# It runs as root on purpose: it installs a browser, and writes results/ into a bind mount owned by
# the host user. It is a development tool and is never deployed.
FROM app AS tests
# hadolint ignore=DL3002
USER 0:0
COPY requirements-test.txt .
# libatomic1: Playwright's bundled node binary needs it and python:3.12-slim does not ship it.
# Separate layers so a stalled download only repeats its own step.
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends libatomic1 \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --timeout 30 --retries 5 -r requirements-test.txt
RUN playwright install --with-deps chromium && rm -rf /var/lib/apt/lists/*
COPY pytest.ini .
COPY tests ./tests
COPY loadtest ./loadtest
CMD ["pytest", "-v"]
