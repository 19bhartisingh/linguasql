"""
╔══════════════════════════════════════════════════════════════╗
║  server.py  —  FastAPI REST API Backend for LinguaSQL v1.0   ║
╚══════════════════════════════════════════════════════════════╝

NEW IN v7.0:
  ★ JWT user authentication — register, login, per-user history
  ★ Scheduled email reports (v6)
  ★ PDF export (v5)
  ★ AI Insights digest (v4)
"""

import os
import re
import sys
import time
import json
import uuid
import sqlite3
from collections import OrderedDict
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile, File, Header, Form
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import io

sys.path.insert(0, os.path.dirname(__file__))

from database import (
    get_all_database_names, get_schema, schema_to_text,
    execute_query, get_database_stats,
    validate_sql, reload_uploaded_databases,
    register_database, unregister_database, is_builtin_database, DATABASE_REGISTRY,
    DUCKDB_AVAILABLE, set_db_engine, get_db_engine,
)
from nl_to_sql import (
    natural_language_to_sql, EXAMPLE_QUERIES, get_provider_status,
    summarize_schema_tables, explain_sql_steps,
    generate_dashboard_plan, generate_insights,
    generate_database_docs, _docs_to_markdown,
    generate_reasoning_trace, detect_schema_industry,
    check_alert_condition,
    DASHBOARD_GOALS,
)
from file_importer import (
    import_file, delete_uploaded_database, SUPPORTED_EXTENSIONS,
)
from sample_databases import setup_all_databases
from pdf_exporter import build_query_pdf, build_dashboard_pdf
from email_reporter import (
    ReportScheduler, WatchdogScheduler, encrypt_key, decrypt_key,
    cron_for_preset, human_readable_cron, _cron_next_run,
    build_html_email, send_email_report, get_smtp_configured,
    CRYPTO_AVAILABLE,
)
from auth import (
    init_users_table, create_user, authenticate_user,
    get_user_by_id, update_user,
    create_token, get_current_user,
)
from external_db import (
    SUPPORTED_DB_TYPES, test_connection, register_external,
    unregister_external, list_external_names, is_external,
    save_connection_to_db, load_connections_from_db,
    delete_connection_from_db, get_connection_str,
    EXTERNAL_MARKER, _EXTERNAL_CONNECTIONS,
)


# ─────────────────────────────────────────────────────────
#  DIALECT-AWARE SQL HELPERS
#  Ensures queries work on SQLite, SQL Server, PostgreSQL, MySQL
# ─────────────────────────────────────────────────────────

def _db_dialect(db_name: str) -> str:
    """Return 'mssql', 'postgresql', 'mysql', or 'sqlite'."""
    try:
        if is_external(db_name):
            dt = _EXTERNAL_CONNECTIONS.get(db_name, {}).get("db_type", "").lower()
            if "mssql" in dt or "sqlserver" in dt or "sql_server" in dt:
                return "mssql"
            if "postgres" in dt:
                return "postgresql"
            if "mysql" in dt or "mariadb" in dt:
                return "mysql"
    except Exception:
        pass
    return "sqlite"


def _qi(name: str, dialect: str) -> str:
    """Quote an identifier for the given SQL dialect."""
    if dialect == "mssql":
        return f"[{name}]"
    if dialect == "mysql":
        return f"`{name}`"
    return f'"{name}"'   # SQLite / PostgreSQL


def _make_select_sql(table: str, columns: list, limit: int,
                     db_name: str, where: str = "") -> str:
    """
    Build a SELECT query using the correct dialect.
    SQL Server : SELECT TOP N [col] FROM [table]
    Others     : SELECT "col" FROM "table" LIMIT N
    """
    dialect     = _db_dialect(db_name)
    tbl_q       = _qi(table, dialect)
    cols_q      = "*" if columns == ["*"] else ", ".join(_qi(c, dialect) for c in columns)
    where_clause = f" WHERE {where}" if where else ""

    if dialect == "mssql":
        return f"SELECT TOP {limit} {cols_q} FROM {tbl_q}{where_clause}"
    return f"SELECT {cols_q} FROM {tbl_q}{where_clause} LIMIT {limit}"


def _adapt_sql_for_dialect(sql: str, db_name: str) -> str:
    """
    Post-process any SQL string to fix dialect-specific syntax.
    Converts LIMIT N → TOP N and double-quoted → [bracket] identifiers for SQL Server.
    """
    dialect = _db_dialect(db_name)
    if dialect != "mssql":
        return sql

    # LIMIT N → TOP N
    limit_m = re.search(r'\bLIMIT\s+(\d+)\b', sql, re.IGNORECASE)
    if limit_m:
        n   = limit_m.group(1)
        sql = re.sub(r'\bLIMIT\s+\d+\b', '', sql, flags=re.IGNORECASE).strip()
        sql = re.sub(r'\bSELECT\b', f'SELECT TOP {n}', sql, count=1, flags=re.IGNORECASE)

    # "double_quoted_identifiers" → [bracket_identifiers]
    sql = re.sub(r'"([^"]+)"', lambda m: f'[{m.group(1)}]', sql)
    return sql


# ─────────────────────────────────────────────────────────
#  APP SETUP
# ─────────────────────────────────────────────────────────

app = FastAPI(title="LinguaSQL API", version="4.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

LOCAL_PROVIDERS = {"ollama", "huggingface"}


# ─────────────────────────────────────────────────────────
#  PERSISTENT META DATABASE
# ─────────────────────────────────────────────────────────

# Railway / Render / Fly.io: prefer a writable persistent path if available
_DATA_DIR = os.environ.get("DATA_DIR") or (
    "/app/databases" if os.path.isdir("/app") else "databases"
)
META_DB_PATH = os.path.join(_DATA_DIR, "linguasql_meta.db")

# Ensure the databases directory always exists
os.makedirs(_DATA_DIR, exist_ok=True)
os.makedirs(os.path.join(_DATA_DIR, "uploads"), exist_ok=True)

# ── Server-side API key fallback ─────────────────────────────────────────────
# If the operator sets these env vars, users don't need to enter their own keys.
_SERVER_API_KEYS: Dict[str, str] = {
    "gemini":  os.environ.get("GEMINI_API_KEY",  ""),
    "openai":  os.environ.get("OPENAI_API_KEY",  ""),
    "groq":    os.environ.get("GROQ_API_KEY",    ""),
}

def _resolve_api_key(provider: str, user_key: str) -> str:
    """Return user key if provided, otherwise fall back to server env key."""
    k = user_key.strip() if user_key else ""
    if k:
        return k
    return _SERVER_API_KEYS.get(provider, "")


def _init_meta_db():
    os.makedirs(_DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(META_DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS query_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question TEXT, sql TEXT, explanation TEXT,
        success INTEGER, rows INTEGER, exec_time_s REAL,
        db_name TEXT, provider TEXT, confidence REAL,
        chart TEXT, timestamp TEXT, user_id INTEGER
    )""")
    # Migrate: add user_id if table already existed without it
    try:
        c.execute("ALTER TABLE query_history ADD COLUMN user_id INTEGER")
    except sqlite3.OperationalError:
        pass
    c.execute("""CREATE TABLE IF NOT EXISTS shared_results (
        uuid TEXT PRIMARY KEY, question TEXT, sql TEXT,
        columns TEXT, rows TEXT, db_name TEXT, timestamp TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS scheduled_reports (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL,
        question    TEXT NOT NULL,
        db_name     TEXT NOT NULL,
        provider    TEXT NOT NULL DEFAULT 'gemini',
        api_key_enc TEXT NOT NULL DEFAULT '',
        schedule_cron TEXT NOT NULL,
        recipient_email TEXT NOT NULL,
        last_run    TEXT,
        next_run    TEXT,
        last_status TEXT,
        active      INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT NOT NULL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS external_connections (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL UNIQUE,
        db_type     TEXT NOT NULL,
        conn_str_enc TEXT NOT NULL,
        created_at  TEXT NOT NULL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS db_docs (
        db_name      TEXT PRIMARY KEY,
        content_json TEXT NOT NULL,
        content_md   TEXT NOT NULL,
        generated_at TEXT NOT NULL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS watchdog_alerts (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        name          TEXT NOT NULL,
        description   TEXT NOT NULL DEFAULT '',
        nl_condition  TEXT NOT NULL,
        sql_query     TEXT NOT NULL,
        db_name       TEXT NOT NULL,
        operator      TEXT NOT NULL DEFAULT '<',
        threshold     REAL NOT NULL DEFAULT 0,
        provider      TEXT NOT NULL DEFAULT 'gemini',
        api_key_enc   TEXT NOT NULL DEFAULT '',
        schedule_cron TEXT NOT NULL DEFAULT '* * * * *',
        recipient_email TEXT NOT NULL DEFAULT '',
        last_value    REAL,
        last_run      TEXT,
        last_status   TEXT,
        triggered     INTEGER NOT NULL DEFAULT 0,
        active        INTEGER NOT NULL DEFAULT 1,
        next_check    TEXT,
        created_at    TEXT NOT NULL
    )""")
    # Migrate existing tables that may lack next_check column
    try:
        c.execute("ALTER TABLE watchdog_alerts ADD COLUMN next_check TEXT")
    except Exception:
        pass   # Column already exists — that's fine
    c.execute("""CREATE TABLE IF NOT EXISTS query_lineage (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        question   TEXT NOT NULL,
        sql        TEXT NOT NULL,
        db_name    TEXT NOT NULL,
        tables_used TEXT NOT NULL DEFAULT '[]',
        cols_used  TEXT NOT NULL DEFAULT '[]',
        timestamp  TEXT NOT NULL
    )""")
    conn.commit()
    conn.close()
    # Users table (handled by auth module)
    init_users_table(META_DB_PATH)


def _save_history_db(question, sql, explanation, success, rows,
                     exec_time, db_name="", provider="", confidence=0.0,
                     chart=None, user_id: Optional[int] = None):
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.execute(
            "INSERT INTO query_history (question,sql,explanation,success,rows,exec_time_s,"
            "db_name,provider,confidence,chart,timestamp,user_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (question, sql, explanation, 1 if success else 0, rows,
             exec_time, db_name, provider, confidence,
             json.dumps(chart or {}), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"History save error: {e}")


def _get_history_db(limit=100, user_id: Optional[int] = None) -> List[Dict]:
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.row_factory = sqlite3.Row
        if user_id is not None:
            rows = conn.execute(
                "SELECT * FROM query_history WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM query_history WHERE user_id IS NULL ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["chart"] = json.loads(d.get("chart") or "{}")
            except Exception:
                d["chart"] = {}
            d["success"] = bool(d.get("success"))
            result.append(d)
        return result
    except Exception:
        return []


def _clear_history_db():
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.execute("DELETE FROM query_history")
        conn.commit()
        conn.close()
    except Exception:
        pass


def _save_shared_result(question, sql, columns, rows, db_name) -> str:
    share_id = str(uuid.uuid4())[:8]
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.execute(
            "INSERT INTO shared_results (uuid,question,sql,columns,rows,db_name,timestamp) VALUES (?,?,?,?,?,?,?)",
            (share_id, question, sql, json.dumps(columns), json.dumps(rows[:200]),
             db_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Share save error: {e}")
    return share_id


def _get_shared_result(share_id: str) -> Optional[Dict]:
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM shared_results WHERE uuid=?", (share_id,)).fetchone()
        conn.close()
        if row:
            d = dict(row)
            d["columns"] = json.loads(d.get("columns") or "[]")
            d["rows"]    = json.loads(d.get("rows") or "[]")
            return d
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────
#  LRU QUERY CACHE
# ─────────────────────────────────────────────────────────

_query_cache: OrderedDict = OrderedDict()
_CACHE_MAX = 200


def _cache_key(question: str, db_name: str, provider: str) -> str:
    return f"{db_name}|{provider}|{question.lower().strip()}"


def _cache_get(key: str):
    if key in _query_cache:
        _query_cache.move_to_end(key)
        return _query_cache[key]
    return None


def _cache_set(key: str, value):
    _query_cache[key] = value
    _query_cache.move_to_end(key)
    if len(_query_cache) > _CACHE_MAX:
        _query_cache.popitem(last=False)


# ─────────────────────────────────────────────────────────
#  REPORT RUNNER  (used by scheduler + /run-now endpoint)
# ─────────────────────────────────────────────────────────

def _execute_report(report: Dict) -> tuple:
    """
    Run a scheduled report end-to-end:
      1. Decrypt API key (or use plaintext key for ad-hoc sends)
      2. Run NL→SQL query
      3. Generate AI insights
      4. Build branded PDF (data + insights — NO SQL shown)
      5. Build HTML email body
      6. Send email with PDF as attachment to all recipients
    Returns (success: bool, message: str)
    """
    import traceback

    question    = report["question"]
    db_name     = report["db_name"]
    provider    = report["provider"]
    report_name = report.get("name", "LinguaSQL Report")

    # Decrypt API key — support both stored (encrypted) and ad-hoc (plaintext)
    raw_enc = report.get("api_key_enc", "")
    if report.get("_plain_api_key"):
        # Ad-hoc send — key was never encrypted
        api_key = raw_enc
    else:
        api_key = decrypt_key(raw_enc) if raw_enc else ""

    email_to = report["recipient_email"]

    # ── Step 1: Get schema ────────────────────────────────
    try:
        schema_text = schema_to_text(db_name)
    except Exception as e:
        return False, f"Schema error for '{db_name}': {e}"

    # ── Step 2: NL → SQL ─────────────────────────────────
    try:
        sql, explanation, confidence, chart = natural_language_to_sql(
            question=question, schema_text=schema_text,
            provider=provider, api_key=api_key,
            db_name=db_name,
        )
    except Exception as e:
        return False, f"AI error: {e}"

    is_safe, msg = validate_sql(sql)
    if not is_safe:
        return False, f"Unsafe SQL blocked: {msg}"

    # ── Step 3: Execute query ─────────────────────────────
    sql = _adapt_sql_for_dialect(sql, db_name)
    df, error = execute_query(db_name, sql)
    if error:
        return False, f"Query error: {error}"

    rows    = _make_serialisable(df.head(500).to_dict(orient="records"))
    columns = list(df.columns)

    if not columns:
        return False, f"Query returned no data. SQL: {sql}"

    # ── Step 4: AI Insights ───────────────────────────────
    try:
        insights = generate_insights(
            question=question, columns=columns, rows=rows[:50],
            provider=provider, api_key=api_key,
        )
    except Exception as e:
        print(f"[Report] Insights warning (non-fatal): {e}")
        insights = []

    now      = datetime.now()
    run_time = now.strftime("%b %d, %Y  %H:%M")
    try:
        next_dt  = _cron_next_run(report["schedule_cron"], after=now)
        next_run = next_dt.strftime("%b %d, %Y  %H:%M")
    except Exception:
        next_run = "—"

    # ── Step 5: Build PDF ─────────────────────────────────
    pdf_bytes    = None
    pdf_filename = f"LinguaSQL_{report_name.replace(' ','_')}_{now.strftime('%Y%m%d')}.pdf"
    try:
        from pdf_exporter import build_query_pdf
        pdf_bytes = build_query_pdf(
            title     = report_name,
            subtitle  = f"Scheduled Report  ·  {run_time}",
            question  = question,
            sql       = "",          # no SQL shown in email PDF
            columns   = columns,
            rows      = rows,
            insights  = insights,
            chart_b64 = None,
            db_name   = db_name,
            show_sql  = False,
        )
        print(f"[Report] PDF built: {len(pdf_bytes):,} bytes, "
              f"{len(rows)} rows, {len(insights)} insights")
    except Exception as e:
        print(f"[Report] PDF build error: {e}")
        traceback.print_exc()
        pdf_bytes = None   # send HTML-only email if PDF fails

    # ── Step 6: Build HTML email body ─────────────────────
    html = build_html_email(
        report_name = report_name,
        question    = question,
        db_name     = db_name,
        sql         = "",           # no SQL in email body
        columns     = columns,
        rows        = rows,
        insights    = insights,
        run_time    = run_time,
        next_run    = next_run,
        total_rows  = len(df),
    )

    # ── Step 7: Send to all recipients ───────────────────
    subject    = f"📊 LinguaSQL Report: {report_name} — {now.strftime('%b %d')}"
    recipients = [e.strip() for e in email_to.split(",") if e.strip()]
    if not recipients:
        return False, "No valid recipient email address specified"

    sent_to   = []
    failed_to = []
    for addr in recipients:
        ok, err = send_email_report(
            to_email     = addr,
            subject      = subject,
            html_body    = html,
            pdf_bytes    = pdf_bytes,
            pdf_filename = pdf_filename,
        )
        if ok:
            sent_to.append(addr)
        else:
            print(f"[Report] Failed to send to {addr}: {err}")
            failed_to.append(f"{addr}: {err}")

    if not sent_to:
        return False, f"SMTP error: {'; '.join(failed_to)}"

    attachment_note = f" (+PDF {len(pdf_bytes)//1024}KB)" if pdf_bytes else " (HTML only — PDF failed)"
    result_msg = f"Sent to {', '.join(sent_to)}{attachment_note}"
    if failed_to:
        result_msg += f" | Failed: {'; '.join(failed_to)}"
    return True, result_msg


# module-level scheduler holder — using a dict avoids Python's
# "used prior to global declaration" SyntaxError in async functions
_schedulers: Dict[str, Any] = {
    "report":   None,   # ReportScheduler instance
    "watchdog": None,   # WatchdogScheduler instance
}


# ─────────────────────────────────────────────────────────
#  STARTUP / SHUTDOWN  (FastAPI lifespan)
# ─────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app):
    # ── STARTUP ──────────────────────────────────────────
    print("\n🚀 LinguaSQL v1.0 starting up...")
    _init_meta_db()
    print("💾 Persistent history database ready")
    db_files = [
        os.path.join(_DATA_DIR, "college.db"),
        os.path.join(_DATA_DIR, "ecommerce.db"),
        os.path.join(_DATA_DIR, "hospital.db"),
    ]
    if not all(os.path.exists(f) for f in db_files):
        print("📦 Creating sample databases...")
        setup_all_databases()
    n = reload_uploaded_databases()
    if n:
        print(f"📂 Restored {n} uploaded database(s)")
    _reload_external_connections()

    _schedulers["report"] = ReportScheduler(META_DB_PATH, _execute_report)
    _schedulers["report"].start()

    _schedulers["watchdog"] = WatchdogScheduler(
        meta_db_path     = META_DB_PATH,
        execute_query_fn = execute_query,
        decrypt_key_fn   = decrypt_key,
        send_email_fn    = send_email_report,
        build_email_fn   = build_html_email,
        adapt_sql_fn     = _adapt_sql_for_dialect,
    )
    _schedulers["watchdog"].start()

    print(f"🔐 Encryption: {'Fernet AES-256' if CRYPTO_AVAILABLE else 'base64'}")
    print(f"📧 SMTP: {'configured' if get_smtp_configured() else 'not set — add SMTP_HOST to env'}")
    print("✅ Ready\n")

    yield   # application runs here

    # ── SHUTDOWN ─────────────────────────────────────────
    for s in _schedulers.values():
        if s:
            s.stop()


app.router.lifespan_context = lifespan

# ─────────────────────────────────────────────────────────
#  FRONTEND
# ─────────────────────────────────────────────────────────

STATIC_DIR    = Path(__file__).parent / "static"
# Railway / Render: index.html may sit next to server.py rather than in static/
_ROOT_HTML    = Path(__file__).parent / "index.html"
FRONTEND_PATH = STATIC_DIR / "index.html" if (STATIC_DIR / "index.html").exists() else _ROOT_HTML

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    if FRONTEND_PATH.exists():
        return HTMLResponse(content=FRONTEND_PATH.read_text(encoding="utf-8"))
    # Last-resort: walk up to find index.html
    for candidate in [Path(__file__).parent / "index.html",
                      Path("/app/index.html"), Path("/app/static/index.html")]:
        if candidate.exists():
            return HTMLResponse(content=candidate.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Frontend not found</h1>", status_code=404)


# ─────────────────────────────────────────────────────────
#  HEALTH
# ─────────────────────────────────────────────────────────

@app.get("/api/health")
async def health_check():
    return {
        "status": "ok", "version": "7.0.0",
        "timestamp": datetime.now().isoformat(),
        "databases": len(DATABASE_REGISTRY),
        "cache_size": len(_query_cache),
    }


# ─────────────────────────────────────────────────────────
#  AUTHENTICATION ENDPOINTS
# ─────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email:    str
    password: str
    name:     str


class LoginRequest(BaseModel):
    email:    str
    password: str


class UpdateMeRequest(BaseModel):
    name:     Optional[str] = None
    password: Optional[str] = None


@app.post("/api/auth/register", status_code=201)
async def register(req: RegisterRequest):
    ok, err, user = create_user(META_DB_PATH, req.email, req.password, req.name)
    if not ok:
        raise HTTPException(400, err)
    token = create_token(user["id"], user["email"], user["name"], user["role"])
    return {"token": token, "user": user}


@app.post("/api/auth/login")
async def login(req: LoginRequest):
    ok, err, user = authenticate_user(META_DB_PATH, req.email, req.password)
    if not ok:
        raise HTTPException(401, err)
    token = create_token(user["id"], user["email"], user["name"], user["role"])
    return {"token": token, "user": user}


@app.get("/api/auth/me")
async def get_me(authorization: Optional[str] = Header(None)):
    user = get_current_user(authorization, META_DB_PATH)
    if not user:
        raise HTTPException(401, "Not authenticated")
    return {"user": user}


@app.put("/api/auth/me")
async def update_me(req: UpdateMeRequest,
                    authorization: Optional[str] = Header(None)):
    user = get_current_user(authorization, META_DB_PATH)
    if not user:
        raise HTTPException(401, "Not authenticated")
    ok, err = update_user(META_DB_PATH, user["id"],
                          name=req.name, password=req.password)
    if not ok:
        raise HTTPException(400, err)
    updated = get_user_by_id(META_DB_PATH, user["id"])
    # Re-issue token with updated name
    token = create_token(updated["id"], updated["email"],
                         updated["name"], updated["role"])
    return {"token": token, "user": updated}






def _reload_external_connections():
    """Reload all saved external connections from meta DB into the in-memory registry."""
    rows = load_connections_from_db(META_DB_PATH)
    loaded = 0
    for row in rows:
        try:
            enc = get_connection_str(META_DB_PATH, row["name"])
            if enc:
                conn_str_enc, db_type = enc
                conn_str = decrypt_key(conn_str_enc)
                display  = f"🔌 {row['name']}"
                register_external(display, db_type, conn_str)
                register_database(display, EXTERNAL_MARKER + row["name"])
                loaded += 1
        except Exception as e:
            print(f"[connections] Failed to restore '{row['name']}': {e}")
    if loaded:
        print(f"🔌 Restored {loaded} external connection(s)")


# ─────────────────────────────────────────────────────────
#  EXTERNAL DB CONNECTIONS
# ─────────────────────────────────────────────────────────

class ConnectRequest(BaseModel):
    name:              str
    db_type:           str
    connection_string: str


@app.post("/api/connect", status_code=201)
async def create_connection(req: ConnectRequest):
    """Test and save an external DB connection."""
    name = req.name.strip()
    if not name:
        raise HTTPException(400, "Connection name is required")
    if req.db_type not in SUPPORTED_DB_TYPES:
        raise HTTPException(400, f"db_type must be one of: {list(SUPPORTED_DB_TYPES)}")
    if not req.connection_string.strip():
        raise HTTPException(400, "Connection string is required")

    display = f"🔌 {name}"
    if display in DATABASE_REGISTRY and not DATABASE_REGISTRY[display].startswith(EXTERNAL_MARKER):
        raise HTTPException(409, f"A database named '{name}' already exists")

    # Test connection before saving
    ok, msg = test_connection(req.db_type, req.connection_string)
    if not ok:
        raise HTTPException(422, f"Connection test failed: {msg}")

    # Encrypt and persist
    enc = encrypt_key(req.connection_string)
    try:
        save_connection_to_db(META_DB_PATH, name, req.db_type, enc)
    except Exception as e:
        if "UNIQUE" in str(e).upper():
            raise HTTPException(409, f"A connection named '{name}' already exists")
        raise HTTPException(500, f"Save failed: {e}")

    # Register in memory
    register_external(display, req.db_type, req.connection_string)
    register_database(display, EXTERNAL_MARKER + name)

    info = SUPPORTED_DB_TYPES[req.db_type]
    return {
        "success":    True,
        "name":       name,
        "display":    display,
        "db_type":    req.db_type,
        "label":      info["label"],
        "icon":       info["icon"],
        "message":    msg,
    }


@app.get("/api/connections")
async def list_connections():
    """List all saved external connections."""
    rows = load_connections_from_db(META_DB_PATH)
    for row in rows:
        info = SUPPORTED_DB_TYPES.get(row["db_type"], {})
        row["label"]   = info.get("label", row["db_type"])
        row["icon"]    = info.get("icon",  "🔌")
        row["display"] = f"🔌 {row['name']}"
    return {"connections": rows, "supported_types": SUPPORTED_DB_TYPES}


@app.delete("/api/connections/{conn_name:path}")
async def delete_connection(conn_name: str):
    """Remove a saved connection."""
    display = f"🔌 {conn_name}"
    deleted_db   = delete_connection_from_db(META_DB_PATH, conn_name)
    unregister_external(display)
    unregister_database(display)
    if not deleted_db:
        raise HTTPException(404, f"Connection '{conn_name}' not found")
    return {"success": True, "deleted": conn_name}


@app.get("/health")
async def health_check():
    """Health check endpoint used by Fly.io, Render, Railway, etc."""
    return {"status": "ok", "app": "LinguaSQL", "version": "1.0"}


@app.get("/health")
async def health_check():
    """Health check for Railway/Fly.io/Render"""
    return {"status": "ok", "app": "LinguaSQL", "version": "1.0"}


@app.get("/api/databases")
async def list_databases():
    names = get_all_database_names()
    return {"databases": names, "count": len(names)}


@app.get("/api/schema/{db_name:path}")
async def get_database_schema(db_name: str):
    try:
        schema = get_schema(db_name)
        stats  = get_database_stats(db_name)
        result = {
            table: {"columns": columns, "row_count": stats.get(table, 0)}
            for table, columns in schema.items()
        }
        return {"db_name": db_name, "schema": result}
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/examples/{db_name:path}")
async def get_example_queries(db_name: str):
    examples = EXAMPLE_QUERIES.get(db_name, [
        "Show all rows", "Count total records",
        "Show the first 10 entries", "Find duplicate rows",
    ])
    goals = DASHBOARD_GOALS.get(db_name, [
        "Give me an overview of this database",
        "Show key metrics and trends",
    ])
    return {"examples": examples, "dashboard_goals": goals}


@app.get("/api/providers")
async def list_providers():
    status = get_provider_status()
    # Mark providers as available if the server has a key configured,
    # even when the user hasn't entered one in the browser.
    providers = status.get("providers", {})
    for name, server_key in _SERVER_API_KEYS.items():
        if server_key and name in providers:
            providers[name]["available"]      = True
            providers[name]["server_key_set"] = True   # UI can show "key provided by server"
    status["server_keys"] = {k: bool(v) for k, v in _SERVER_API_KEYS.items()}
    return status


# ─────────────────────────────────────────────────────────
#  MAIN QUERY ENDPOINT
# ─────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question:             str
    db_name:              str
    provider:             str
    api_key:              str           = ""
    model:                Optional[str] = None
    max_rows:             int           = 5000
    conversation_history: List[Dict]    = []


@app.post("/api/query")
async def run_natural_language_query(req: QueryRequest,
                                     authorization: Optional[str] = Header(None)):
    if not req.question.strip():
        raise HTTPException(400, "Question cannot be empty")

    # Resolve API key: user-supplied key takes priority, then server env key
    effective_key = _resolve_api_key(req.provider, req.api_key)
    if req.provider not in LOCAL_PROVIDERS and not effective_key:
        # Friendly warning instead of hard error — no key available anywhere
        return {
            "error": (
                "⚠️ No API key configured. "
                "To use LinguaSQL, enter your free Gemini API key in the sidebar "
                "(get one at aistudio.google.com — it's free). "
                "Or the site owner can set GEMINI_API_KEY in the server environment."
            ),
            "sql": "", "explanation": "", "confidence": 0, "rows": [], "columns": [],
        }

    # Resolve current user (None = anonymous)
    current_user = get_current_user(authorization, META_DB_PATH)
    user_id      = current_user["id"] if current_user else None

    # LRU cache (skip for conversation mode — context differs)
    cache_k = _cache_key(req.question, req.db_name, req.provider)
    if not req.conversation_history:
        cached = _cache_get(cache_k)
        if cached:
            return {**cached, "from_cache": True}

    start = time.time()

    try:
        schema_text = schema_to_text(req.db_name)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, f"Schema error: {e}")

    try:
        sql, explanation, confidence, chart = natural_language_to_sql(
            question             = req.question,
            schema_text          = schema_text,
            provider             = req.provider,
            api_key              = effective_key,
            model                = req.model,
            conversation_history = req.conversation_history or None,
            retry_on_low_confidence = True,
            db_name              = req.db_name,
        )
    except Exception as e:
        raise HTTPException(500, f"AI error: {e}")

    is_safe, safety_msg = validate_sql(sql)
    if not is_safe:
        raise HTTPException(400, f"Unsafe query blocked: {safety_msg}")

    # Adapt SQL syntax for the connected database dialect
    # (e.g. converts LIMIT N → TOP N for SQL Server)
    sql = _adapt_sql_for_dialect(sql, req.db_name)

    df, error = execute_query(req.db_name, sql)
    exec_time  = round(time.time() - start, 3)

    if error:
        _save_history_db(req.question, sql, explanation, False, 0,
                         exec_time, req.db_name, req.provider, confidence, chart,
                         user_id=user_id)
        raise HTTPException(400, f"SQL execution failed: {error}")

    rows    = _make_serialisable(df.head(req.max_rows).to_dict(orient="records"))
    columns = list(df.columns)

    result = {
        "sql": sql, "explanation": explanation,
        "confidence": round(confidence, 3), "chart": chart,
        "columns": columns, "rows": rows,
        "total_rows": len(df), "returned_rows": len(rows),
        "exec_time_s": exec_time, "from_cache": False,
    }

    _save_history_db(req.question, sql, explanation, True, len(df),
                     exec_time, req.db_name, req.provider, confidence, chart,
                     user_id=user_id)
    _cache_set(cache_k, result)

    # Track query lineage (best-effort, non-blocking)
    try:
        import re as _re
        tables_used = list(set(_re.findall(r'FROM\s+"?(\w+)"?|JOIN\s+"?(\w+)"?', sql.upper())))
        tables_flat = [t for pair in tables_used for t in pair if t]
        conn_l = sqlite3.connect(META_DB_PATH)
        conn_l.execute(
            "INSERT INTO query_lineage (question,sql,db_name,tables_used,cols_used,timestamp) VALUES(?,?,?,?,?,?)",
            (req.question, sql, req.db_name, json.dumps(tables_flat),
             json.dumps(columns[:20]), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn_l.commit(); conn_l.close()
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────────────────
#  RAW SQL EXECUTION
# ─────────────────────────────────────────────────────────

class ExecuteRequest(BaseModel):
    sql: str; db_name: str; max_rows: int = 200


@app.post("/api/execute")
async def execute_raw_sql(req: ExecuteRequest):
    is_safe, msg = validate_sql(req.sql)
    if not is_safe:
        raise HTTPException(400, msg)
    # Adapt SQL syntax for the connected database (LIMIT→TOP, "quotes"→[brackets] for SQL Server)
    adapted_sql = _adapt_sql_for_dialect(req.sql, req.db_name)
    df, error = execute_query(req.db_name, adapted_sql)
    if error:
        raise HTTPException(400, error)
    return {
        "columns": list(df.columns),
        "rows": _make_serialisable(df.head(req.max_rows).to_dict(orient="records")),
        "total_rows": len(df),
    }


# ─────────────────────────────────────────────────────────
#  FILE IMPORT
# ─────────────────────────────────────────────────────────

@app.post("/api/import")
async def import_dataset(file: UploadFile = File(...),
                         use_duckdb: str = Form("false")):
    filename   = file.filename or "upload.csv"
    extension  = os.path.splitext(filename)[1].lower()
    if extension not in SUPPORTED_EXTENSIONS:
        raise HTTPException(400, f"Unsupported format '{extension}'")
    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(400, "File is empty")
    if len(file_bytes) > 200 * 1024 * 1024:
        raise HTTPException(400, "File too large (max 200 MB)")

    want_duckdb = use_duckdb.lower() in ("1", "true", "yes")

    result = import_file(file_bytes=file_bytes, filename=filename,
                         use_duckdb=want_duckdb)
    if not result["success"]:
        raise HTTPException(422, result.get("error", "Import failed"))

    return {
        "success":          True,
        "db_name":          result["db_name"],
        "tables":           result["tables"],
        "message":          result["message"],
        "file_format":      SUPPORTED_EXTENSIONS.get(extension, extension),
        "engine":           result.get("engine", "sqlite"),
        "duckdb_available": DUCKDB_AVAILABLE,
        "suggest_duckdb":   result.get("suggest_duckdb", False),
    }


@app.delete("/api/database/{db_name:path}")
async def delete_database(db_name: str):
    if is_builtin_database(db_name):
        raise HTTPException(403, "Cannot delete built-in sample databases")
    db_path = DATABASE_REGISTRY.get(db_name)
    if not db_path:
        raise HTTPException(404, f"Database '{db_name}' not found")
    if delete_uploaded_database(db_path):
        unregister_database(db_name)
        return {"success": True, "message": f"Deleted '{db_name}'"}
    raise HTTPException(500, "Failed to delete database file")


# ─────────────────────────────────────────────────────────
#  HISTORY  (persistent SQLite)
# ─────────────────────────────────────────────────────────

@app.get("/api/history")
async def get_query_history():
    return {"history": _get_history_db(100)}


@app.delete("/api/history")
async def clear_query_history():
    _clear_history_db()
    _query_cache.clear()
    return {"success": True}


# ─────────────────────────────────────────────────────────
#  DATA PROFILING
# ─────────────────────────────────────────────────────────

@app.get("/api/profile/{db_name:path}")
async def profile_database(db_name: str):
    try:
        schema = get_schema(db_name)
        stats  = get_database_stats(db_name)
        profile = {}
        for table, cols in schema.items():
            row_count = stats.get(table, 0)
            table_profile = {}
            for col in cols:
                cname = col["name"]
                ctype = col["type"]
                col_stat: Dict[str, Any] = {"type": ctype, "total_rows": row_count}
                try:
                    df, err = execute_query(db_name, f'SELECT "{cname}" FROM "{table}"')
                    if not err and df is not None:
                        series = df[cname]
                        col_stat["non_null"] = int(series.notna().sum())
                        col_stat["null_pct"] = round(series.isna().mean() * 100, 1)
                        col_stat["unique"]   = int(series.nunique())
                        try:
                            import pandas as pd
                            num = pd.to_numeric(series.dropna(), errors="coerce").dropna()
                            if len(num) > 0:
                                col_stat["min"]  = round(float(num.min()), 4)
                                col_stat["max"]  = round(float(num.max()), 4)
                                col_stat["mean"] = round(float(num.mean()), 4)
                        except Exception:
                            pass
                        try:
                            top = series.dropna().value_counts().head(5)
                            col_stat["top_values"] = [
                                {"value": str(k), "count": int(v)} for k, v in top.items()
                            ]
                        except Exception:
                            pass
                except Exception:
                    pass
                table_profile[cname] = col_stat
            profile[table] = table_profile
        return {"db_name": db_name, "profile": profile}
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────────────────
#  CORRELATION MATRIX
# ─────────────────────────────────────────────────────────

@app.get("/api/correlate/{db_name}/{table_name}")
async def get_correlation_matrix(db_name: str, table_name: str,
                                  max_cols: int = 10):
    """
    Compute a Pearson correlation matrix for numeric columns in a table.

    Returns:
        {
          "db_name": "...", "table_name": "...",
          "columns": ["col_a", "col_b", ...],
          "matrix": [[1.0, 0.82, ...], [0.82, 1.0, ...], ...],
          "highlights": [{"col_a": "col_b", "r": 0.82, "strength": "strong"}]
        }
    """
    import pandas as pd
    import numpy as np

    try:
        # Fetch all columns for the table
        df, error = execute_query(db_name, _make_select_sql(table_name, ["*"], 50000, db_name))
        if error:
            raise HTTPException(400, f"Query error: {error}")
        if df is None or df.empty:
            raise HTTPException(404, f"Table '{table_name}' is empty or not found")
    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))

    # Select numeric columns (coerce to numeric, drop all-NaN)
    numeric_df = df.apply(pd.to_numeric, errors="coerce")
    numeric_df = numeric_df.dropna(axis=1, how="all")
    numeric_cols = [c for c in numeric_df.columns if numeric_df[c].notna().sum() >= 3]

    if len(numeric_cols) < 2:
        return {
            "db_name": db_name, "table_name": table_name,
            "columns": [], "matrix": [],
            "highlights": [],
            "message": "Need at least 2 numeric columns with data to compute correlations.",
        }

    # Cap at max_cols for readability
    numeric_cols = numeric_cols[:max_cols]
    sub = numeric_df[numeric_cols].dropna()

    if len(sub) < 3:
        return {
            "db_name": db_name, "table_name": table_name,
            "columns": [], "matrix": [], "highlights": [],
            "message": "Not enough non-null rows to compute correlations (need ≥ 3).",
        }

    corr = sub.corr(method="pearson")

    # Round to 3 decimal places; replace NaN with 0
    matrix = [[round(float(v), 3) if not np.isnan(v) else 0.0
               for v in row]
              for row in corr.values]

    # Find notable correlations (|r| > 0.5, exclude diagonal)
    highlights = []
    cols = list(corr.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            r = matrix[i][j]
            if abs(r) >= 0.5:
                strength = "very strong" if abs(r) >= 0.9 else \
                           "strong"      if abs(r) >= 0.7 else "moderate"
                direction = "positive" if r > 0 else "negative"
                highlights.append({
                    "col_a":     cols[i],
                    "col_b":     cols[j],
                    "r":         r,
                    "strength":  strength,
                    "direction": direction,
                })
    highlights.sort(key=lambda h: abs(h["r"]), reverse=True)

    return {
        "db_name":    db_name,
        "table_name": table_name,
        "columns":    cols,
        "matrix":     matrix,
        "row_count":  len(sub),
        "highlights": highlights[:10],
    }


# ─────────────────────────────────────────────────────────
#  DATA CLEANING
# ─────────────────────────────────────────────────────────

from data_cleaner import (
    analyze_table, apply_cleaning,
    df_to_csv_bytes, df_to_cleaned_sqlite, build_cleaning_pdf,
)


class CleanAnalyzeRequest(BaseModel):
    db_name:    str
    table_name: str


class CleanApplyRequest(BaseModel):
    db_name:    str
    table_name: str
    operations: List[str]
    params:     Dict = {}
    save_as:    str  = ""   # new display name to save cleaned DB as


@app.post("/api/clean/analyze")
async def clean_analyze(req: CleanAnalyzeRequest):
    """Scan a table for data quality issues — returns structured report."""
    df, err = execute_query(req.db_name, _make_select_sql(req.table_name, ["*"], 100000, req.db_name))
    if err:
        raise HTTPException(400, f"Query failed: {err}")
    if df is None or df.empty:
        raise HTTPException(404, "Table is empty or not found")
    report = analyze_table(df, req.table_name)
    return report


@app.post("/api/clean/apply")
async def clean_apply(req: CleanApplyRequest):
    """Apply cleaning, write back to source DB, return preview + fresh scan."""
    if not req.operations:
        raise HTTPException(400, "No operations specified")

    df, err = execute_query(req.db_name, _make_select_sql(req.table_name, ["*"], 100000, req.db_name))
    if err:
        raise HTTPException(400, f"Query failed: {err}")
    if df is None or df.empty:
        raise HTTPException(404, "Table is empty")

    original_shape = df.shape
    cleaned_df, log = apply_cleaning(df, req.operations, req.params)

    # ── Always write cleaned data back to the SOURCE database ──
    # This makes the change permanent so re-scanning shows updated results.
    write_error = None
    try:
        db_path = DATABASE_REGISTRY.get(req.db_name, "")
        if db_path and not db_path.startswith("external:") and db_path.endswith((".db", ".sqlite", ".sqlite3")):
            import sqlite3 as _sqlite3
            conn = _sqlite3.connect(db_path)
            cleaned_df.to_sql(req.table_name, conn, if_exists="replace", index=False, chunksize=500)
            conn.commit()
            conn.close()
            log.append(f"💾 Changes saved to '{req.db_name}' → table '{req.table_name}'")
        else:
            write_error = "Source database is read-only or external — use Save As to create a copy."
            log.append(f"⚠️ Could not write back to source ({write_error}). Use Save As.")
    except Exception as e:
        write_error = str(e)
        log.append(f"⚠️ Write-back failed: {e}")

    # ── Optionally also save as a NEW named database ──
    saved_db_name = ""
    if req.save_as.strip():
        from file_importer import clean_column_name
        base    = clean_column_name(req.save_as.strip()) or "cleaned"
        path    = os.path.join(_DATA_DIR, "uploads", f"{base}.db")
        df_to_cleaned_sqlite(cleaned_df, req.table_name, path)
        display = f"📄 {base}"
        register_database(display, path)
        set_db_engine(display, "sqlite", path)
        saved_db_name = display
        log.append(f"💾 Also saved as new database: '{display}'")

    # ── Re-scan cleaned data so caller gets updated issue list ──
    fresh_report = analyze_table(cleaned_df, req.table_name)

    rows_preview = _make_serialisable(cleaned_df.head(200).to_dict(orient="records"))

    return {
        "original_rows": original_shape[0],
        "original_cols": original_shape[1],
        "cleaned_rows":  len(cleaned_df),
        "cleaned_cols":  len(cleaned_df.columns),
        "log":           log,
        "columns":       list(cleaned_df.columns),
        "rows":          rows_preview,
        "saved_db_name": saved_db_name,
        "write_error":   write_error,
        "fresh_report":  fresh_report,   # updated issue list after cleaning
    }


@app.post("/api/clean/export/csv")
async def clean_export_csv(req: CleanApplyRequest):
    """Apply operations and stream result as CSV download."""
    df, err = execute_query(req.db_name, _make_select_sql(req.table_name, ["*"], 100000, req.db_name))
    if err:
        raise HTTPException(400, err)

    cleaned_df, _ = apply_cleaning(df, req.operations, req.params)
    csv_bytes      = df_to_csv_bytes(cleaned_df)
    filename       = f"{req.table_name}_cleaned.csv"
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/clean/export/pdf")
async def clean_export_pdf(req: CleanApplyRequest):
    """Apply operations and stream a branded PDF cleaning report."""
    df, err = execute_query(req.db_name, _make_select_sql(req.table_name, ["*"], 100000, req.db_name))
    if err:
        raise HTTPException(400, err)

    original_shape = df.shape
    cleaned_df, log = apply_cleaning(df, req.operations, req.params)
    issues_applied  = []   # could be enriched later

    pdf_bytes = build_cleaning_pdf(
        table_name     = req.table_name,
        original_shape = original_shape,
        cleaned_shape  = cleaned_df.shape,
        operations     = req.operations,
        log            = log,
        issues_applied = issues_applied,
    )

    content_type = "application/pdf" if pdf_bytes[:4] == b"%PDF" else "text/plain"
    filename     = f"{req.table_name}_cleaning_report.pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )



class SchemaSummaryRequest(BaseModel):
    db_name: str; provider: str; api_key: str = ""; model: Optional[str] = None


@app.post("/api/schema-summary")
async def get_schema_summary(req: SchemaSummaryRequest):
    try:
        schema_text = schema_to_text(req.db_name)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    eff_key = _resolve_api_key(req.provider, req.api_key)
    summaries = summarize_schema_tables(schema_text, req.provider, eff_key, req.model)
    return {"db_name": req.db_name, "summaries": summaries}


# ─────────────────────────────────────────────────────────
#  SQL STEP EXPLAINER
# ─────────────────────────────────────────────────────────

class ExplainRequest(BaseModel):
    sql: str; db_name: str; provider: str; api_key: str = ""; model: Optional[str] = None


@app.post("/api/explain-steps")
async def explain_sql_step_by_step(req: ExplainRequest):
    is_safe, msg = validate_sql(req.sql)
    if not is_safe:
        raise HTTPException(400, msg)
    try:
        schema_text = schema_to_text(req.db_name)
    except Exception:
        schema_text = ""
    eff_key = _resolve_api_key(req.provider, req.api_key)
    steps = explain_sql_steps(req.sql, schema_text, req.provider, eff_key, req.model)
    return {"steps": steps}


# ─────────────────────────────────────────────────────────
#  AI INSIGHTS
# ─────────────────────────────────────────────────────────

class InsightsRequest(BaseModel):
    question:  str
    columns:   List[str]
    rows:      List[Dict]
    provider:  str
    api_key:   str           = ""
    model:     Optional[str] = None


@app.post("/api/insights")
async def get_query_insights(req: InsightsRequest):
    if not req.rows or not req.columns:
        return {"insights": []}
    effective_key = _resolve_api_key(req.provider, req.api_key)
    if req.provider not in LOCAL_PROVIDERS and not effective_key:
        return {"insights": [{"emoji": "⚠️", "text": "⚠️ No API key configured. Enter your free Gemini API key in the sidebar (get one at aistudio.google.com). The site owner can also set GEMINI_API_KEY as an environment variable."}]}
    insights = generate_insights(
        question = req.question,
        columns  = req.columns,
        rows     = req.rows,
        provider = req.provider,
        api_key  = effective_key,
        model    = req.model,
    )
    return {"insights": insights}


# ─────────────────────────────────────────────────────────
#  AUTO-DOCUMENTATION
# ─────────────────────────────────────────────────────────

class GenerateDocsRequest(BaseModel):
    db_name:  str
    provider: str
    api_key:  str           = ""
    model:    Optional[str] = None


def _save_docs(db_name: str, content_json: str, content_md: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(META_DB_PATH)
    conn.execute(
        """INSERT INTO db_docs (db_name, content_json, content_md, generated_at)
           VALUES (?,?,?,?)
           ON CONFLICT(db_name) DO UPDATE SET
             content_json=excluded.content_json,
             content_md=excluded.content_md,
             generated_at=excluded.generated_at""",
        (db_name, content_json, content_md, now)
    )
    conn.commit(); conn.close()


def _load_docs(db_name: str) -> Optional[Dict]:
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM db_docs WHERE db_name=?", (db_name,)
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["content_json"] = json.loads(d["content_json"])
            except Exception: pass
            return d
    except Exception:
        pass
    return None


@app.post("/api/docs/generate")
async def generate_docs(req: GenerateDocsRequest):
    """Generate AI documentation for a database and persist it."""
    effective_key = _resolve_api_key(req.provider, req.api_key)
    if req.provider not in LOCAL_PROVIDERS and not effective_key:
        raise HTTPException(400, "⚠️ No API key configured. Enter your free Gemini API key in the sidebar (get one at aistudio.google.com). The site owner can also set GEMINI_API_KEY as an environment variable.")
    try:
        schema_dict = get_schema(req.db_name)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))

    schema_text = schema_to_text(req.db_name)

    # Collect sample rows for each table (up to 3 rows per table)
    sample_data: Dict[str, List[Dict]] = {}
    for table in schema_dict:
        df, err = execute_query(req.db_name, _make_select_sql(table, ["*"], 3, req.db_name))
        if not err and df is not None:
            sample_data[table] = _make_serialisable(df.to_dict(orient="records"))

    try:
        docs = generate_database_docs(
            schema_text = schema_text,
            schema_dict = schema_dict,
            sample_data = sample_data,
            provider    = req.provider,
            api_key     = effective_key,
            model       = req.model,
        )
    except Exception as e:
        raise HTTPException(500, str(e))

    md = _docs_to_markdown(req.db_name, docs)
    _save_docs(req.db_name, json.dumps(docs), md)

    return {
        "db_name":      req.db_name,
        "docs":         docs,
        "markdown":     md,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


@app.get("/api/docs/{db_name:path}")
async def get_docs(db_name: str):
    """Return previously generated documentation for a database."""
    saved = _load_docs(db_name)
    if not saved:
        raise HTTPException(404, f"No documentation found for '{db_name}'. Generate it first.")
    return saved


@app.get("/api/docs/{db_name:path}/markdown")
async def get_docs_markdown(db_name: str):
    """Return raw Markdown documentation as a downloadable file."""
    saved = _load_docs(db_name)
    if not saved:
        raise HTTPException(404, f"No documentation found for '{db_name}'.")
    filename = _safe_filename(db_name) + "_docs.md"
    return StreamingResponse(
        io.BytesIO(saved["content_md"].encode()),
        media_type="text/markdown",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ══════════════════════════════════════════════════════════
#  INTELLIGENCE LAYER — 4 UNIQUE FEATURES
# ══════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────
#  FEATURE 1: SQL REASONING TRACE
# ─────────────────────────────────────────────────────────

class ReasoningTraceRequest(BaseModel):
    question:   str
    db_name:    str
    provider:   str
    api_key:    str = ""
    model:      Optional[str] = None


@app.post("/api/reasoning-trace")
async def get_reasoning_trace(req: ReasoningTraceRequest):
    """Generate AI chain-of-thought reasoning before SQL generation."""
    if not req.question.strip():
        raise HTTPException(400, "Question is required")
    try:
        schema_text = schema_to_text(req.db_name)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))

    effective_key = _resolve_api_key(req.provider, req.api_key)
    if req.provider not in LOCAL_PROVIDERS and not effective_key:
        return {"question": req.question, "trace": []}
    trace = generate_reasoning_trace(
        question    = req.question,
        schema_text = schema_text,
        provider    = req.provider,
        api_key     = effective_key,
        model       = req.model,
    )
    return {"question": req.question, "trace": trace}


# ─────────────────────────────────────────────────────────
#  FEATURE 2: NL WATCHDOG ALERTS
# ─────────────────────────────────────────────────────────

class WatchdogCreateRequest(BaseModel):
    name:             str
    description:      str = ""
    nl_condition:     str              # "alert me when daily orders < 50"
    sql_query:        str              # pre-generated or supplied SQL
    db_name:          str
    operator:         str = "<"        # <, >, <=, >=, ==, !=
    threshold:        float = 0
    provider:         str = "gemini"
    api_key:          str = ""
    schedule_cron:    str = "0 * * * *"
    recipient_email:  str = ""


def _run_watchdog_check(alert: Dict) -> Dict:
    """Execute a watchdog alert SQL and check its condition. Returns status dict."""
    try:
        # Support both 'sql_query' (DB row) and 'sql_query' (request)
        sql = alert.get("sql_query") or alert.get("sql", "")
        db  = alert.get("db_name", "")
        if not sql:
            return {"triggered": False, "value": None, "error": "No SQL query defined for this alert"}
        if not db:
            return {"triggered": False, "value": None, "error": "No database selected for this alert"}

        df, err = execute_query(db, sql)
        if err:
            return {"triggered": False, "value": None, "error": f"Query error: {err}"}
        if df is None or df.empty:
            return {"triggered": False, "value": 0, "error": None}

        # The query should return a single numeric value
        raw = df.iloc[0, 0]
        try:
            val = float(raw)
        except (TypeError, ValueError):
            return {"triggered": False, "value": None,
                    "error": f"Query returned non-numeric value: {raw!r}. SQL must return a single number."}

        triggered = check_alert_condition(val, alert.get("operator", "<"), alert.get("threshold", 0))
        return {"triggered": triggered, "value": val, "error": None}
    except Exception as e:
        return {"triggered": False, "value": None, "error": str(e)}


@app.post("/api/watchdog", status_code=201)
async def create_watchdog(req: WatchdogCreateRequest):
    """Create a new watchdog alert."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    enc_key = encrypt_key(req.api_key) if req.api_key else ""
    conn = sqlite3.connect(META_DB_PATH)
    cur = conn.execute(
        """INSERT INTO watchdog_alerts
           (name,description,nl_condition,sql_query,db_name,operator,threshold,
            provider,api_key_enc,schedule_cron,recipient_email,active,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,1,?)""",
        (req.name, req.description, req.nl_condition, req.sql_query, req.db_name,
         req.operator, req.threshold, req.provider, enc_key,
         req.schedule_cron, req.recipient_email, now)
    )
    alert_id = cur.lastrowid
    conn.commit(); conn.close()

    # Run immediately to get initial value
    alert_dict = req.dict()
    status = _run_watchdog_check(alert_dict)
    return {"id": alert_id, "name": req.name, "initial_check": status}


@app.get("/api/watchdog")
async def list_watchdogs():
    """List all watchdog alerts."""
    conn = sqlite3.connect(META_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id,name,description,nl_condition,sql_query,db_name,operator,"
        "threshold,schedule_cron,recipient_email,last_value,last_run,"
        "last_status,triggered,active,created_at FROM watchdog_alerts ORDER BY id DESC"
    ).fetchall()
    conn.close()
    return {"alerts": [dict(r) for r in rows]}


@app.post("/api/watchdog/{alert_id}/run")
async def run_watchdog_now(alert_id: int):
    """Manually trigger a watchdog check and update its status."""
    conn = sqlite3.connect(META_DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM watchdog_alerts WHERE id=?", (alert_id,)).fetchone()
    if not row:
        conn.close(); raise HTTPException(404, "Alert not found")
    alert = dict(row)
    conn.close()

    status = _run_watchdog_check(alert)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_str = "TRIGGERED" if status["triggered"] else ("ERROR" if status["error"] else "OK")

    conn = sqlite3.connect(META_DB_PATH)
    conn.execute(
        "UPDATE watchdog_alerts SET last_value=?,last_run=?,last_status=?,triggered=? WHERE id=?",
        (status["value"], now, status_str, 1 if status["triggered"] else 0, alert_id)
    )
    conn.commit(); conn.close()
    return {"id": alert_id, "status": status_str, **status}


@app.delete("/api/watchdog/{alert_id}")
async def delete_watchdog(alert_id: int):
    conn = sqlite3.connect(META_DB_PATH)
    conn.execute("DELETE FROM watchdog_alerts WHERE id=?", (alert_id,))
    conn.commit(); conn.close()
    return {"success": True, "deleted": alert_id}


@app.patch("/api/watchdog/{alert_id}/toggle")
async def toggle_watchdog(alert_id: int):
    conn = sqlite3.connect(META_DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT active FROM watchdog_alerts WHERE id=?", (alert_id,)).fetchone()
    if not row: conn.close(); raise HTTPException(404, "Not found")
    new_active = 0 if row["active"] else 1
    conn.execute("UPDATE watchdog_alerts SET active=? WHERE id=?", (new_active, alert_id))
    conn.commit(); conn.close()
    return {"id": alert_id, "active": bool(new_active)}


class WatchdogGenerateSQLRequest(BaseModel):
    nl_condition: str
    db_name:      str
    provider:     str
    api_key:      str = ""


@app.post("/api/watchdog/generate-sql")
async def watchdog_generate_sql(req: WatchdogGenerateSQLRequest):
    """
    Convert a plain-English alert condition into a single-value SQL query.
    Uses the correct SQL dialect for the target database.
    Returns {"sql": "SELECT COUNT(*) FROM ..."}
    """
    if not req.nl_condition.strip():
        raise HTTPException(400, "Condition text is required")
    if not req.db_name.strip():
        raise HTTPException(400, "Database name is required")

    try:
        schema_text = schema_to_text(req.db_name)
    except Exception as e:
        raise HTTPException(404, f"Schema error: {e}")

    # Detect dialect for the correct date functions
    from nl_to_sql import _get_db_dialect, _DIALECT_HINTS
    dialect = _get_db_dialect(req.db_name)
    dialect_hint = _DIALECT_HINTS.get(dialect, _DIALECT_HINTS["sqlite"])

    watchdog_prompt = f"""You are a SQL expert. Convert this monitoring condition into a SQL query.

Database schema:
{schema_text}

SQL dialect rules: {dialect_hint}

Condition to monitor: "{req.nl_condition}"

Write a SELECT query that returns EXACTLY ONE numeric value representing the condition.
Examples:
- "alert when orders today exceed 100" → SELECT COUNT(*) FROM orders WHERE CAST(order_date AS DATE) = CAST(GETDATE() AS DATE)
- "alert when average price drops below 50" → SELECT AVG(price) FROM products
- "alert when stock count falls below 10" → SELECT SUM(quantity) FROM inventory

IMPORTANT:
- Return ONLY the SQL query, nothing else, no explanation, no markdown.
- Use the correct dialect functions for dates (not strftime if SQL Server).
- The query MUST return exactly 1 row with 1 numeric column.
"""

    try:
        effective_key = _resolve_api_key(req.provider, req.api_key)
        sql, _, _, _ = natural_language_to_sql(
            question    = watchdog_prompt,
            schema_text = schema_text,
            provider    = req.provider,
            api_key     = effective_key,
            db_name     = req.db_name,
        )
        # Strip any LIMIT that was added (watchdog needs full count)
        sql = re.sub(r'\bLIMIT\s+\d+\b', '', sql, flags=re.IGNORECASE).strip()
        return {"sql": sql, "dialect": dialect}
    except Exception as e:
        raise HTTPException(500, f"SQL generation failed: {e}")


# ─────────────────────────────────────────────────────────
#  FEATURE 3: QUERY DNA / LINEAGE GRAPH
# ─────────────────────────────────────────────────────────

@app.get("/api/lineage/{db_name:path}")
async def get_lineage(db_name: str):
    """
    Returns schema relationships + query history for a Query DNA graph.
    Combines static FK relationships with dynamic query patterns.
    """
    try:
        schema = get_schema(db_name)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))

    # Build nodes (tables)
    nodes = []
    for tbl, cols in schema.items():
        nodes.append({
            "id":    tbl,
            "label": tbl,
            "cols":  len(cols),
            "pks":   [c["name"] for c in cols if c.get("pk")],
        })

    # Build edges from FK relationships — multi-strategy detection
    edges = []
    seen_edges = set()
    table_names = list(schema.keys())

    def _add_edge(src, tgt, col, etype):
        ek = tuple(sorted([src, tgt]))
        if ek not in seen_edges and src != tgt:
            edges.append({"source": src, "target": tgt, "col": col, "type": etype})
            seen_edges.add(ek)

    for tbl, cols in schema.items():
        for col in cols:
            col_name = col["name"].lower()

            # Strategy A: explicit FK flag from schema
            if col.get("fk"):
                # Try to match which table it references by name
                for other in table_names:
                    if other == tbl:
                        continue
                    other_l = other.lower()
                    if (col_name == f"{other_l}_id" or
                        col_name == f"{other_l[:-1]}_id" or
                        col_name.startswith(other_l + "_") or
                        other_l.startswith(col_name.replace("_id", ""))):
                        _add_edge(tbl, other, col["name"], "fk")
                        break

            # Strategy B: _id suffix heuristic — even without explicit FK flag
            if col_name.endswith("_id") and not col.get("pk"):
                ref = col_name[:-3]  # strip _id
                for other in table_names:
                    if other == tbl:
                        continue
                    other_l = other.lower()
                    # Match many plural/singular forms
                    if (other_l == ref or                              # exact
                        other_l == ref + "s" or                       # simple plural
                        other_l == ref + "es" or                      # -es plural
                        other_l.rstrip("s") == ref or                 # strip s
                        other_l.replace("ies", "y") == ref or         # categories→category
                        ref.replace("ies", "y") == other_l or         # reverse
                        ref in other_l or                             # ref contained in table
                        other_l.startswith(ref[:max(3, len(ref)-2)])):# prefix match (min 3 chars)
                        _add_edge(tbl, other, col["name"], "fk_inferred")
                        break

    # Strategy C: co-occurrence from query history
    # Tables frequently queried together likely have a relationship
    try:
        conn = sqlite3.connect(META_DB_PATH)
        rows = conn.execute(
            "SELECT tables_used FROM query_lineage WHERE db_name=? ORDER BY id DESC LIMIT 200",
            (db_name,)
        ).fetchall()
        conn.close()
        from itertools import combinations
        pair_counts: Dict[tuple, int] = {}
        for (tables_json,) in rows:
            try:
                tbls = [t for t in json.loads(tables_json) if t in schema]
                for pair in combinations(sorted(set(tbls)), 2):
                    pair_counts[pair] = pair_counts.get(pair, 0) + 1
            except Exception:
                pass
        # Add edges for pairs co-queried ≥ 2 times (strong signal)
        for (a, b), cnt in pair_counts.items():
            if cnt >= 2:
                _add_edge(a, b, f"co-queried {cnt}x", "co_query")
    except Exception:
        pass

    # Query lineage — how many queries touched each table
    try:
        conn = sqlite3.connect(META_DB_PATH)
        rows = conn.execute(
            "SELECT tables_used, COUNT(*) as cnt FROM query_lineage WHERE db_name=? GROUP BY tables_used",
            (db_name,)
        ).fetchall()
        conn.close()

        table_query_count: Dict[str, int] = {}
        for tables_json, cnt in rows:
            try:
                for t in json.loads(tables_json):
                    t_lower = t.lower()
                    for node in nodes:
                        if node["id"].lower() == t_lower:
                            table_query_count[node["id"]] = table_query_count.get(node["id"], 0) + cnt
            except Exception:
                pass

        for node in nodes:
            node["query_count"] = table_query_count.get(node["id"], 0)
    except Exception:
        for node in nodes:
            node["query_count"] = 0

    # Recent queries for timeline
    try:
        conn = sqlite3.connect(META_DB_PATH)
        recent = conn.execute(
            "SELECT question, sql, tables_used, timestamp FROM query_lineage "
            "WHERE db_name=? ORDER BY id DESC LIMIT 20",
            (db_name,)
        ).fetchall()
        conn.close()
        recent_queries = [
            {"question": r[0], "sql": r[1],
             "tables": json.loads(r[2]) if r[2] else [], "timestamp": r[3]}
            for r in recent
        ]
    except Exception:
        recent_queries = []

    return {
        "db_name": db_name,
        "nodes":   nodes,
        "edges":   edges,
        "recent_queries": recent_queries,
    }


# ─────────────────────────────────────────────────────────
#  FEATURE 4: SMART SCHEMA DETECTIVE
# ─────────────────────────────────────────────────────────

class SchemaDetectiveRequest(BaseModel):
    db_name:  str
    provider: str
    api_key:  str = ""
    model:    Optional[str] = None


@app.post("/api/schema-detective")
async def schema_detective(req: SchemaDetectiveRequest):
    """Detect industry from schema and return context-aware example questions."""
    try:
        schema_text = schema_to_text(req.db_name)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))

    effective_key = _resolve_api_key(req.provider, req.api_key)
    result = detect_schema_industry(
        schema_text = schema_text,
        provider    = req.provider,
        api_key     = effective_key,
        model       = req.model,
    )
    if not result:
        raise HTTPException(500, "Could not detect schema industry")

    # Persist as examples for this database (so they appear in the hints)
    if "example_questions" in result:
        EXAMPLE_QUERIES[req.db_name] = result["example_questions"]
    if "dashboard_goals" in result:
        from nl_to_sql import DASHBOARD_GOALS
        DASHBOARD_GOALS[req.db_name] = result["dashboard_goals"]

    return {"db_name": req.db_name, **result}


class ExportQueryPdfRequest(BaseModel):
    title:     str
    subtitle:  str          = ""
    question:  str          = ""
    sql:       str          = ""
    columns:   List[str]    = []
    rows:      List[Dict]   = []
    insights:  List[Dict]   = []
    chart_b64: str          = ""   # ECharts getDataURL() output (data:image/png;base64,...)
    db_name:   str          = ""


@app.post("/api/export-pdf")
async def export_query_pdf(req: ExportQueryPdfRequest):
    """Generate a branded PDF for a single query result and return it as a download."""
    try:
        pdf_bytes = build_query_pdf(
            title     = req.title or "Query Report",
            subtitle  = req.subtitle,
            sql       = req.sql,
            columns   = req.columns,
            rows      = req.rows,
            insights  = req.insights,
            chart_b64 = req.chart_b64 or None,
            db_name   = req.db_name,
            question  = req.question,
        )
    except Exception as e:
        raise HTTPException(500, f"PDF generation failed: {e}")

    filename = _safe_filename(req.title or "linguasql_report") + ".pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


class ExportDashboardPdfRequest(BaseModel):
    title:      str
    subtitle:   str          = ""
    goal:       str          = ""
    db_name:    str          = ""
    theme:      str          = "blue"
    kpi_row:    List[Dict]   = []
    panels:     List[Dict]   = []   # each panel may have chart_b64 field


@app.post("/api/export-dashboard-pdf")
async def export_dashboard_pdf(req: ExportDashboardPdfRequest):
    """Generate a branded multi-panel dashboard PDF and return it as a download."""
    try:
        pdf_bytes = build_dashboard_pdf(
            title    = req.title or "Dashboard Report",
            subtitle = req.subtitle,
            goal     = req.goal,
            db_name  = req.db_name,
            theme    = req.theme,
            kpi_row  = req.kpi_row,
            panels   = req.panels,
        )
    except Exception as e:
        raise HTTPException(500, f"Dashboard PDF generation failed: {e}")

    filename = _safe_filename(req.title or "linguasql_dashboard") + ".pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _safe_filename(title: str) -> str:
    """Convert a title to a safe ASCII filename."""
    import re
    s = title.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "_", s)
    return s[:60] or "report"


# ─────────────────────────────────────────────────────────
#  SCHEDULED REPORTS
# ─────────────────────────────────────────────────────────

class ScheduleRequest(BaseModel):
    name:            str
    question:        str
    db_name:         str
    provider:        str        = "gemini"
    api_key:         str        = ""
    frequency:       str        = "daily"   # daily|weekly|monthly|hourly|<cron>
    hour:            int        = 8
    minute:          int        = 0
    weekday:         int        = 1         # 0=Mon … 6=Sun (used for weekly)
    recipient_email: str


@app.post("/api/reports/schedule")
async def create_scheduled_report(req: ScheduleRequest):
    """Create a new scheduled report."""
    if not req.name.strip():
        raise HTTPException(400, "Report name required")
    if not req.question.strip():
        raise HTTPException(400, "Question required")
    if not req.recipient_email.strip():
        raise HTTPException(400, "Recipient email required")

    cron = cron_for_preset(req.frequency, req.hour, req.minute, req.weekday)
    try:
        next_run = _cron_next_run(cron)
    except Exception as e:
        raise HTTPException(400, f"Invalid schedule: {e}")

    api_key_enc = encrypt_key(req.api_key) if req.api_key else ""
    now_str     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    next_str    = next_run.strftime("%Y-%m-%d %H:%M:%S")

    try:
        conn = sqlite3.connect(META_DB_PATH)
        cur  = conn.execute(
            """INSERT INTO scheduled_reports
               (name,question,db_name,provider,api_key_enc,schedule_cron,
                recipient_email,last_run,next_run,last_status,active,created_at)
               VALUES (?,?,?,?,?,?,?,NULL,?,NULL,1,?)""",
            (req.name, req.question, req.db_name, req.provider, api_key_enc,
             cron, req.recipient_email, next_str, now_str)
        )
        new_id = cur.lastrowid
        conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")

    return {
        "id":            new_id,
        "name":          req.name,
        "cron":          cron,
        "schedule_human": human_readable_cron(cron),
        "next_run":      next_str,
        "active":        True,
    }


@app.get("/api/reports/schedule")
async def list_scheduled_reports():
    """Return all scheduled reports (API keys redacted)."""
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM scheduled_reports ORDER BY id DESC"
        ).fetchall()
        conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    result = []
    for r in rows:
        d = dict(r)
        d.pop("api_key_enc", None)   # never expose encrypted key
        d["active"]         = bool(d.get("active", 1))
        d["schedule_human"] = human_readable_cron(d.get("schedule_cron", ""))
        d["smtp_ready"]     = get_smtp_configured()
        result.append(d)
    return {"reports": result, "smtp_configured": get_smtp_configured()}


@app.delete("/api/reports/schedule/{report_id}")
async def delete_scheduled_report(report_id: int):
    """Permanently delete a scheduled report."""
    try:
        conn = sqlite3.connect(META_DB_PATH)
        affected = conn.execute(
            "DELETE FROM scheduled_reports WHERE id=?", (report_id,)
        ).rowcount
        conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    if not affected:
        raise HTTPException(404, f"Report {report_id} not found")
    return {"success": True, "deleted_id": report_id}


@app.patch("/api/reports/schedule/{report_id}/toggle")
async def toggle_scheduled_report(report_id: int):
    """Pause / resume a scheduled report."""
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT active FROM scheduled_reports WHERE id=?", (report_id,)
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, f"Report {report_id} not found")
        new_active = 0 if row["active"] else 1
        # If re-activating, recompute next_run
        if new_active:
            cron_row = conn.execute(
                "SELECT schedule_cron FROM scheduled_reports WHERE id=?", (report_id,)
            ).fetchone()
            try:
                next_run = _cron_next_run(cron_row["schedule_cron"])
                next_str = next_run.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                next_str = None
            if next_str:
                conn.execute(
                    "UPDATE scheduled_reports SET active=?, next_run=? WHERE id=?",
                    (new_active, next_str, report_id)
                )
            else:
                conn.execute(
                    "UPDATE scheduled_reports SET active=? WHERE id=?",
                    (new_active, report_id)
                )
        else:
            conn.execute(
                "UPDATE scheduled_reports SET active=? WHERE id=?",
                (new_active, report_id)
            )
        conn.commit()
        conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
    return {"success": True, "id": report_id, "active": bool(new_active)}


class RunReportRequest(BaseModel):
    override_emails: Optional[str] = None   # comma-separated extra recipients


@app.post("/api/reports/run/{report_id}")
async def run_report_now(report_id: int, req: RunReportRequest = RunReportRequest()):
    """Manually trigger a scheduled report immediately.
       Pass override_emails to send to a different/additional address.
    """
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM scheduled_reports WHERE id=?", (report_id,)
        ).fetchone()
        conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))

    if not row:
        raise HTTPException(404, f"Report {report_id} not found")

    report = dict(row)

    # Allow caller to override/add recipient emails
    if req.override_emails and req.override_emails.strip():
        report["recipient_email"] = req.override_emails.strip()

    ok, msg = _execute_report(report)

    # Update last_run regardless of success
    now_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status   = "ok" if ok else f"error: {msg}"
    try:
        next_run = _cron_next_run(report["schedule_cron"])
        next_str = next_run.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        next_str = now_str
    try:
        conn = sqlite3.connect(META_DB_PATH)
        conn.execute(
            "UPDATE scheduled_reports SET last_run=?, next_run=?, last_status=? WHERE id=?",
            (now_str, next_str, status, report_id)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

    if not ok:
        raise HTTPException(500, f"Report run failed: {msg}")
    return {"success": True, "message": msg, "last_run": now_str}



class SendNowRequest(BaseModel):
    """Instant ad-hoc email send — no schedule, no stored report needed."""
    question:  str
    db_name:   str
    provider:  str
    api_key:   str          = ""
    to_emails: str          = ""    # comma-separated recipients
    subject:   str          = ""    # optional custom subject


@app.post("/api/send-report-now")
async def send_report_now(req: SendNowRequest):
    """
    Instantly run a query and email the results (data + insights + PDF)
    to one or more email addresses — no schedule required.
    """
    if not req.to_emails.strip():
        raise HTTPException(400, "At least one recipient email is required")
    if not req.question.strip():
        raise HTTPException(400, "Question is required")
    effective_key = _resolve_api_key(req.provider, req.api_key)
    if req.provider not in LOCAL_PROVIDERS and not effective_key:
        raise HTTPException(400, "⚠️ No API key configured. Enter your free Gemini API key in the sidebar (get one at aistudio.google.com). The site owner can also set GEMINI_API_KEY as an environment variable.")

    # Build a synthetic report dict and reuse _execute_report
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report = {
        "name":            req.subject or f"LinguaSQL Report — {req.question[:60]}",
        "question":        req.question,
        "db_name":         req.db_name,
        "provider":        req.provider,
        "api_key_enc":     effective_key,  # plain key
        "_plain_api_key":  True,           # tells _execute_report NOT to decrypt
        "recipient_email": req.to_emails.strip(),
        "schedule_cron":   "0 9 * * *",   # dummy cron (unused)
    }

    ok, msg = _execute_report(report)
    if not ok:
        raise HTTPException(500, f"Send failed: {msg}")

    recipients = [e.strip() for e in req.to_emails.split(",") if e.strip()]
    return {
        "success":    True,
        "message":    msg,
        "recipients": recipients,
        "sent_at":    now_str,
    }


class ShareRequest(BaseModel):
    question: str          = ""
    sql:      str          = ""
    columns:  List[str]    = []
    rows:     List[Dict]   = []
    db_name:  str          = ""



@app.post("/api/share")
async def create_share_link(req: ShareRequest):
    share_id = _save_shared_result(req.question, req.sql, req.columns, req.rows, req.db_name)
    return {"uuid": share_id, "url": f"/api/share/{share_id}"}


@app.get("/api/share/{share_id}")
async def get_shared_result_endpoint(share_id: str):
    result = _get_shared_result(share_id)
    if not result:
        raise HTTPException(404, f"Shared result '{share_id}' not found")
    return result


# ─────────────────────────────────────────────────────────
#  TEXT-TO-DASHBOARD
# ─────────────────────────────────────────────────────────

class DashboardRequest(BaseModel):
    goal: str; db_name: str; provider: str
    api_key: str = ""; model: Optional[str] = None; max_rows: int = 100


@app.post("/api/dashboard")
async def generate_dashboard(req: DashboardRequest):
    if not req.goal.strip():
        raise HTTPException(400, "Goal cannot be empty")
    effective_key = _resolve_api_key(req.provider, req.api_key)
    if req.provider not in LOCAL_PROVIDERS and not effective_key:
        raise HTTPException(400, "⚠️ No API key configured. Enter your free Gemini API key in the sidebar (get one at aistudio.google.com). The site owner can also set GEMINI_API_KEY as an environment variable.")
    try:
        schema_text = schema_to_text(req.db_name)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    try:
        plan = generate_dashboard_plan(req.goal, schema_text, req.provider, effective_key, req.model)
    except Exception as e:
        raise HTTPException(500, f"Dashboard plan error: {e}")

    # Execute KPI row queries
    kpi_row = []
    for kpi in plan.get("kpi_row", []):
        sql = kpi.get("sql", "")
        if not sql:
            continue
        is_safe, _ = validate_sql(sql)
        if not is_safe:
            continue
        df, error = execute_query(req.db_name, sql)
        if not error and df is not None and len(df) > 0:
            kpi["columns"] = list(df.columns)
            kpi["rows"]    = _make_serialisable(df.head(1).to_dict(orient="records"))
            kpi_row.append(kpi)

    # Execute chart panels
    panels = []
    for panel in plan.get("panels", []):
        sql = panel.get("sql", "")
        if not sql:
            continue
        is_safe, _ = validate_sql(sql)
        if not is_safe:
            continue
        df, error = execute_query(req.db_name, sql)
        if error or df is None:
            panel["error"] = error
        else:
            panel["columns"]    = list(df.columns)
            panel["rows"]       = _make_serialisable(df.head(req.max_rows).to_dict(orient="records"))
            panel["total_rows"] = len(df)
        panels.append(panel)

    return {
        "dashboard_title":  plan.get("dashboard_title", "Dashboard"),
        "subtitle":         plan.get("subtitle", ""),
        "dashboard_theme":  plan.get("dashboard_theme", "blue"),
        "goal":             req.goal,
        "db_name":          req.db_name,
        "kpi_row":          kpi_row,
        "panels":           panels,
    }


# ─────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────

def _make_serialisable(rows: list) -> list:
    import numpy as np
    clean = []
    for row in rows:
        clean_row = {}
        for k, v in row.items():
            if isinstance(v, np.integer):   v = int(v)
            elif isinstance(v, np.floating): v = None if np.isnan(v) else float(v)
            elif isinstance(v, np.bool_):    v = bool(v)
            elif isinstance(v, float) and v != v: v = None
            clean_row[k] = v
        clean.append(clean_row)
    return clean


# ─────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    print("=" * 54)
    print("  🌐  LinguaSQL v1.0 — Natural Language to SQL")
    print(f"  🚀  Listening on 0.0.0.0:{port}")
    print("=" * 54)
    uvicorn.run(
        app,                  # pass app object directly — avoids module reload issues
        host      = "0.0.0.0",
        port      = port,
        reload    = False,
        log_level = "info",
        workers   = 1,        # single worker (SQLite is not multiprocess-safe)
    )