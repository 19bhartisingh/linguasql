# ════════════════════════════════════════════════════
#  LinguaSQL — Dockerfile
#  Optimised for Railway, Fly.io, Render
# ════════════════════════════════════════════════════

FROM python:3.11-slim

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    freetds-dev \
    freetds-bin \
    libpq-dev \
    libfreetype6-dev \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Python dependencies ───────────────────────────────────────────────────────
COPY requirements.txt .

# Install everything except pymssql first (always succeeds)
RUN pip install --no-cache-dir $(grep -v pymssql requirements.txt | grep -v '^#' | grep -v '^$' | tr '\n' ' ')

# pymssql separately — failure here won't break the rest of the app
RUN pip install --no-cache-dir pymssql==2.3.1 || \
    echo "WARNING: pymssql install failed — MS SQL Server connections unavailable"

# ── Application files ─────────────────────────────────────────────────────────
COPY . .

# ── Frontend: put index.html where server.py expects it ───────────────────────
# server.py looks for static/index.html first, then falls back to root index.html
# This step copies root index.html → static/index.html so both locations work.
RUN mkdir -p static && \
    if [ -f index.html ]; then \
        cp index.html static/index.html && \
        echo "✅ Copied index.html → static/index.html"; \
    elif [ -f static/index.html ]; then \
        echo "✅ static/index.html already in place"; \
    else \
        echo "❌ WARNING: No index.html found anywhere — UI will return 404"; \
    fi

# ── Database directories ──────────────────────────────────────────────────────
RUN mkdir -p databases databases/uploads

# ── Environment ───────────────────────────────────────────────────────────────
# Do NOT set PORT here — Railway injects it at runtime automatically
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

# ── Health check ─────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request, os; urllib.request.urlopen('http://localhost:' + os.environ.get('PORT','8000') + '/health')"

CMD ["python", "server.py"]