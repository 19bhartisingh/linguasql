# ════════════════════════════════════════════════════
#  LinguaSQL — Dockerfile
#  Optimised for Railway, Render, Fly.io
# ════════════════════════════════════════════════════

FROM python:3.11-slim

# System deps: libpq for psycopg2, freetype for reportlab, gcc for compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    libfreetype6-dev \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps — skip pymssql (needs freetds, optional)
COPY requirements.txt .
RUN pip install --no-cache-dir $(grep -v pymssql requirements.txt | grep -v '^#' | grep -v '^$' | tr '\n' ' ') \
    && pip install --no-cache-dir pymssql==2.3.1 \
    || echo "WARNING: pymssql unavailable — SQL Server connections disabled"

# Copy all application files
COPY . .

# Put index.html into static/ so FastAPI can find it
RUN mkdir -p static && \
    if [ -f index.html ]; then \
        cp index.html static/index.html && echo "✅ index.html → static/"; \
    fi

# Create database directories
RUN mkdir -p databases databases/uploads

# Railway injects PORT at runtime — default to 8000 for local/Dockerfile use
ENV PORT=8000

# Expose default port (Railway overrides this via PORT env var)
EXPOSE 8000

# Health check against the correct endpoint
HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request, os; urllib.request.urlopen('http://localhost:' + os.environ.get('PORT','8000') + '/api/health')"

CMD ["python", "server.py"]