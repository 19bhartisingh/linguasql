"""
external_db.py — Multi-database connection layer for LinguaSQL

Supports:
  • SQLite files      — stdlib sqlite3 (always available)
  • PostgreSQL        — requires: pip install psycopg2-binary
  • MySQL / MariaDB   — requires: pip install pymysql
  • MS SQL Server     — 4 strategies tried automatically (pymssql preferred, no ODBC needed)
  • DuckDB            — requires: pip install duckdb

MS SQL Server connection strategy (auto-tried in order):
  1. pymssql direct   — no ODBC driver needed, pure Python
  2. SQLAlchemy + pymssql
  3. pyodbc + auto-detected ODBC driver (17 or 18)
  4. SQLAlchemy + pyodbc

Architecture:
  - Connections are stored encrypted in linguasql_meta.db.
  - At runtime: _EXTERNAL_CONNECTIONS registry maps display_name → {db_type, conn_str}.
  - database.py routes any db_name whose DATABASE_REGISTRY value starts with "external:"
    through get_external_schema() and execute_external_query() here.
"""

import os
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, quote_plus

import pandas as pd

# ── Optional driver imports ───────────────────────────────────────────────────

def _try_import(module: str):
    try:
        return __import__(module)
    except ImportError:
        return None

_sa       = _try_import("sqlalchemy")
_psycopg2 = _try_import("psycopg2")
_pymysql  = _try_import("pymysql")
_pyodbc   = _try_import("pyodbc")
_pymssql  = _try_import("pymssql")   # no ODBC needed — recommended for SQL Server
_duckdb   = _try_import("duckdb")


# ── DB type registry ──────────────────────────────────────────────────────────

SUPPORTED_DB_TYPES = {
    "postgresql": {
        "label":    "PostgreSQL",
        "icon":     "🐘",
        "example":  "postgresql://user:password@host:5432/dbname",
        "requires": "psycopg2-binary",
        "driver":   "psycopg2-binary",
    },
    "mysql": {
        "label":    "MySQL / MariaDB",
        "icon":     "🐬",
        "example":  "mysql://user:password@host:3306/dbname",
        "requires": "pymysql",
        "driver":   "pymysql",
    },
    "mssql": {
        "label":    "MS SQL Server",
        "icon":     "🏢",
        "example":  "MYPC\\SQLEXPRESS/DatabaseName",
        "requires": "pymssql  (recommended — no ODBC needed)",
        "driver":   "pymssql",
        "hint":     "pip install pymssql  — no ODBC driver installation needed",
    },
    "sqlite_file": {
        "label":    "SQLite File",
        "icon":     "📂",
        "example":  "/absolute/path/to/database.db  OR  relative/path.db",
        "requires": None,
        "driver":   None,
    },
    "duckdb": {
        "label":    "DuckDB",
        "icon":     "🦆",
        "example":  "/absolute/path/to/file.duckdb  OR  :memory:",
        "requires": "duckdb",
        "driver":   "duckdb",
    },
}

# In-memory registry: display_name → {db_type, conn_str}
_EXTERNAL_CONNECTIONS: Dict[str, Dict] = {}

# Marker used in DATABASE_REGISTRY to flag external connections
EXTERNAL_MARKER = "external:"


# ── Registry helpers ──────────────────────────────────────────────────────────

def register_external(name: str, db_type: str, conn_str: str) -> None:
    """Register an external connection so database.py can route to it."""
    _EXTERNAL_CONNECTIONS[name] = {"db_type": db_type, "conn_str": conn_str}


def unregister_external(name: str) -> None:
    _EXTERNAL_CONNECTIONS.pop(name, None)


def is_external(db_name: str) -> bool:
    return db_name in _EXTERNAL_CONNECTIONS


def list_external_names() -> List[str]:
    return list(_EXTERNAL_CONNECTIONS.keys())


# ── MSSQL: universal connection string parser ─────────────────────────────────

def _parse_mssql_parts(conn_str: str) -> Dict[str, str]:
    """
    Extract server, database, user, password from ANY MSSQL connection string.
    Handles: SQLAlchemy URL, ODBC key=value, simple SERVER\\INSTANCE/database.
    """
    cs = conn_str.strip()
    result = {"server": "", "database": "master", "user": "", "password": "", "instance": ""}

    # SQLAlchemy URL
    if "://" in cs:
        try:
            bare = re.sub(r'^[a-z+]+://', 'mssql://', cs)
            p = urlparse(bare)
            srv = (p.hostname or "").replace("%5c", "\\").replace("%5C", "\\")
            if "\\" in srv:
                result["server"], result["instance"] = srv.split("\\", 1)
            else:
                result["server"] = srv
            db_path = (p.path or "").lstrip("/").split("?")[0]
            if db_path:
                result["database"] = db_path
            if p.username:
                result["user"] = p.username
            if p.password:
                result["password"] = p.password
        except Exception:
            pass
        return result

    # ODBC key=value string
    if "=" in cs and ";" in cs:
        for part in cs.split(";"):
            kv = part.strip()
            if "=" not in kv:
                continue
            k, v = kv.split("=", 1)
            k = k.strip().lower()
            v = v.strip().strip("{}")
            if k == "server":
                if "\\" in v:
                    result["server"], result["instance"] = v.split("\\", 1)
                else:
                    result["server"] = v
            elif k == "database":
                result["database"] = v
            elif k in ("uid", "username", "user id"):
                result["user"] = v
            elif k in ("pwd", "password"):
                result["password"] = v
        return result

    # Simple: SERVER\INSTANCE/database
    cs_clean = cs.replace("\\\\", "\\")
    if "/" in cs_clean:
        srv_part, db_part = cs_clean.split("/", 1)
        result["database"] = db_part.strip() or "master"
    else:
        srv_part = cs_clean

    if "\\" in srv_part:
        srv, inst = srv_part.strip().split("\\", 1)
        result["server"]   = srv.strip()
        result["instance"] = inst.strip()
    else:
        result["server"] = srv_part.strip()

    return result


def _get_best_odbc_driver() -> Optional[str]:
    """Return best available SQL Server ODBC driver name, or None."""
    if not _pyodbc:
        return None
    try:
        drivers = [d for d in _pyodbc.drivers() if "SQL Server" in d]
    except Exception:
        return None
    if not drivers:
        return None
    for d in reversed(sorted(drivers)):
        if "18" in d:
            return d
    for d in reversed(sorted(drivers)):
        if "17" in d:
            return d
    return drivers[-1]


def _open_mssql(conn_str: str):
    """
    Try 4 strategies in order. Returns (engine_or_conn, method_str) or raises.
    Strategy 1: pymssql         — no ODBC, pure Python TCP connection
    Strategy 2: SA + pymssql    — SQLAlchemy with pymssql dialect
    Strategy 3: pyodbc direct   — needs ODBC Driver 17/18
    Strategy 4: SA + pyodbc     — SQLAlchemy with pyodbc dialect
    """
    p = _parse_mssql_parts(conn_str)
    server      = p["server"]
    db          = p["database"] or "master"
    user        = p["user"]
    pwd         = p["password"]
    inst        = p["instance"]
    server_full = f"{server}\\{inst}" if inst else server

    errors = []

    # ── Strategy 1: pymssql direct ───────────────────────────────────────────
    if _pymssql:
        try:
            kwargs = {"server": server_full, "database": db, "timeout": 15}
            if user and pwd:
                kwargs["user"]     = user
                kwargs["password"] = pwd
            # Windows Auth: omit credentials — pymssql handles it automatically.
            # Do NOT pass trusted=True — that keyword does not exist in pymssql.
            conn = _pymssql.connect(**kwargs)
            return conn, "pymssql (no ODBC needed)"
        except Exception as e:
            errors.append(f"pymssql: {e}")

    # ── Strategy 2: SQLAlchemy + pymssql ────────────────────────────────────
    if _sa and _pymssql:
        try:
            if user and pwd:
                url = (f"mssql+pymssql://{quote_plus(user)}:{quote_plus(pwd)}"
                       f"@{server_full}/{db}")
            else:
                url = f"mssql+pymssql://{server_full}/{db}"
            eng = _sa.create_engine(url, pool_pre_ping=True, pool_timeout=15,
                                     connect_args={"timeout": 15})
            with eng.connect() as c:
                c.execute(_sa.text("SELECT 1"))
            return eng, "SQLAlchemy+pymssql"
        except Exception as e:
            errors.append(f"SA+pymssql: {e}")

    # ── Strategy 3: pyodbc direct ────────────────────────────────────────────
    odbc_drv = _get_best_odbc_driver()
    if _pyodbc and odbc_drv:
        try:
            if user and pwd:
                odbc = (f"DRIVER={{{odbc_drv}}};SERVER={server_full};"
                        f"DATABASE={db};UID={user};PWD={pwd};"
                        f"TrustServerCertificate=yes;Connection Timeout=15;")
            else:
                odbc = (f"DRIVER={{{odbc_drv}}};SERVER={server_full};"
                        f"DATABASE={db};Trusted_Connection=yes;"
                        f"TrustServerCertificate=yes;Connection Timeout=15;")
            conn = _pyodbc.connect(odbc, timeout=15)
            return conn, f"pyodbc ({odbc_drv})"
        except Exception as e:
            errors.append(f"pyodbc({odbc_drv}): {e}")

    # ── Strategy 4: SQLAlchemy + pyodbc ─────────────────────────────────────
    if _sa and _pyodbc and odbc_drv:
        try:
            drv_enc = odbc_drv.replace(" ", "+")
            if user and pwd:
                url = (f"mssql+pyodbc://{quote_plus(user)}:{quote_plus(pwd)}"
                       f"@{server_full}/{db}?driver={drv_enc}"
                       f"&TrustServerCertificate=yes")
            else:
                url = (f"mssql+pyodbc://{server_full}/{db}?driver={drv_enc}"
                       f"&trusted_connection=yes&TrustServerCertificate=yes")
            eng = _sa.create_engine(url, pool_pre_ping=True, pool_timeout=15)
            with eng.connect() as c:
                c.execute(_sa.text("SELECT 1"))
            return eng, f"SQLAlchemy+pyodbc ({odbc_drv})"
        except Exception as e:
            errors.append(f"SA+pyodbc: {e}")

    # ── All strategies failed ─────────────────────────────────────────────────
    hints = []

    # Detect local/private hostnames — these are unreachable from cloud deployments
    _srv_bare = server_full.split("\\")[0].lower()
    _local_indicators = ["localhost", "127.0.0.1", ".", "(local)"]
    _is_private_ip = _srv_bare.startswith(("10.", "172.", "192.168."))
    _is_local_name = (
        any(ind in _srv_bare for ind in _local_indicators)
        or (
            "." not in _srv_bare          # no dots = Windows PC hostname, not a DNS name
            and not _is_private_ip
            and _srv_bare not in ("", "localhost")
        )
    )

    if _is_local_name or _is_private_ip:
        hints.append(
            f"⚠️  CLOUD DEPLOYMENT ISSUE: '{server_full}' appears to be a local PC or "
            f"private network machine. This app is running on a cloud server which CANNOT "
            f"reach SQL Server instances on your local computer or private network.\n\n"
            f"To connect your local SQL Server you have 3 options:\n"
            f"  1. Use a cloud-hosted SQL Server (Azure SQL, AWS RDS, Supabase, etc.)\n"
            f"  2. Expose your local SQL Server publicly using ngrok:\n"
            f"     • Run: ngrok tcp 1433\n"
            f"     • Use the ngrok host as your server address\n"
            f"  3. Run LinguaSQL locally (python server.py) — it can reach {server_full} directly"
        )
    else:
        if not _pymssql:
            hints.append("QUICKEST FIX: pip install pymssql  (no ODBC driver needed)")
        if not odbc_drv:
            hints.append("OR install ODBC Driver 17: https://aka.ms/odbc17  then restart LinguaSQL")

    raise ConnectionError(
        f"Could not connect to SQL Server \'{server_full}/{db}\'.\n"
        + ("\n".join(hints) + "\n" if hints else "")
        + "\nAttempted:\n" + "\n".join(f"  • {e}" for e in errors)
    )


# ── Universal engine open ─────────────────────────────────────────────────────

def _open_engine(db_type: str, conn_str: str) -> Tuple[Any, str]:
    """
    Open and return (engine_or_connection, method_description).
    For MSSQL, tries 4 strategies automatically.
    Always returns a tuple — callers must unpack [0].
    """
    dt = db_type.lower().strip()

    # ── SQLite file ───────────────────────────────────────────────────────────
    if dt == "sqlite_file":
        path = os.path.abspath(conn_str.strip())
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQLite file not found: {path}")
        if _sa:
            return _sa.create_engine(
                f"sqlite:///{path}",
                connect_args={"check_same_thread": False}
            ), "SQLAlchemy+sqlite3"
        return sqlite3.connect(path), "sqlite3"

    # ── DuckDB ────────────────────────────────────────────────────────────────
    if dt == "duckdb":
        if not _duckdb:
            raise ImportError("DuckDB not installed. Run: pip install duckdb")
        db_path = conn_str.strip() or ":memory:"
        return _duckdb.connect(db_path, read_only=True), "duckdb"

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    if dt in ("postgresql", "postgres"):
        url = conn_str.strip()
        if _sa:
            if not url.startswith("postgresql+"):
                url = (url
                       .replace("postgresql://", "postgresql+psycopg2://", 1)
                       .replace("postgres://",   "postgresql+psycopg2://", 1))
            return _sa.create_engine(url, pool_pre_ping=True), "SQLAlchemy+psycopg2"
        if _psycopg2:
            return _psycopg2.connect(conn_str), "psycopg2"
        raise ImportError("Run: pip install psycopg2-binary")

    # ── MySQL / MariaDB ───────────────────────────────────────────────────────
    if dt in ("mysql", "mariadb"):
        url = conn_str.strip()
        if _sa:
            if not url.startswith("mysql+"):
                url = url.replace("mysql://", "mysql+pymysql://", 1)
            return _sa.create_engine(url, pool_pre_ping=True), "SQLAlchemy+pymysql"
        if _pymysql:
            p = urlparse(conn_str)
            return _pymysql.connect(
                host=p.hostname, port=p.port or 3306,
                user=p.username, password=p.password,
                database=p.path.lstrip("/"),
            ), "pymysql"
        raise ImportError("Run: pip install pymysql")

    # ── MS SQL Server — 4-strategy auto-fallback ──────────────────────────────
    if dt in ("mssql", "sqlserver", "sql_server"):
        return _open_mssql(conn_str)

    raise ValueError(f"Unsupported db_type: {dt!r}. Supported: {list(SUPPORTED_DB_TYPES)}")


# ── Connection test ───────────────────────────────────────────────────────────

def test_connection(db_type: str, conn_str: str) -> Tuple[bool, str]:
    """Try to open a connection and run SELECT 1. Returns (success, message)."""
    try:
        result = _open_engine(db_type, conn_str)
        engine, method = result if isinstance(result, tuple) else (result, "")

        if isinstance(engine, sqlite3.Connection):
            engine.execute("SELECT 1")
            engine.close()
        elif _sa and hasattr(engine, "connect") and not hasattr(engine, "cursor"):
            with engine.connect() as c:
                c.execute(_sa.text("SELECT 1"))
            engine.dispose()
        elif hasattr(engine, "cursor"):
            # pymssql / pyodbc / psycopg2 / pymysql native connection
            cur = engine.cursor()
            cur.execute("SELECT 1")
            cur.close()
            engine.close()
        elif _duckdb and isinstance(engine, _duckdb.DuckDBPyConnection):
            engine.execute("SELECT 1")
            engine.close()

        note = f" via {method}" if method else ""
        return True, f"✅ Connection successful{note}"

    except Exception as e:
        err = str(e)
        if "IM002" in err or "Data source name not found" in err:
            return False, (
                "❌ ODBC Driver not found.\n"
                "QUICKEST FIX: pip install pymssql  (no ODBC driver needed)\n"
                "Then restart LinguaSQL and try again."
            )
        if "Login failed" in err or "18456" in err:
            return False, "❌ Login failed — wrong SQL Server username or password."
        if "Cannot open database" in err:
            return False, (
                "❌ Database not found in SQL Server.\n"
                "Check the Database Name field — use the exact name from SSMS."
            )
        if "network-related" in err.lower() or "10061" in err or "10060" in err:
            return False, (
                "❌ Cannot reach SQL Server.\n"
                "Check: 1) SQL Server service is running  "
                "2) Server\\Instance name is correct  "
                "3) TCP/IP enabled in SQL Server Configuration Manager"
            )
        if "No module named" in err or "ModuleNotFoundError" in err:
            return False, f"❌ Missing driver: {err}\nRun: pip install pymssql"
        return False, f"❌ Connection failed: {err}"


# ── Schema extraction ─────────────────────────────────────────────────────────

def get_external_schema(db_type: str, conn_str: str) -> Dict[str, List[Dict]]:
    """
    Extract table/column metadata. Returns same format as database.get_schema().
    """
    dt = db_type.lower()
    result_tuple = _open_engine(dt, conn_str)
    engine = result_tuple[0] if isinstance(result_tuple, tuple) else result_tuple

    try:
        # ── pymssql native connection (has cursor but not SA) ────────────────
        # Must check before the generic hasattr(engine, "cursor") catch-all
        if _pymssql and isinstance(engine, _pymssql.Connection):
            return _schema_via_pymssql(engine)

        # ── SQLAlchemy engine ────────────────────────────────────────────────
        if _sa and hasattr(engine, "connect") and not hasattr(engine, "cursor"):
            return _schema_via_sqlalchemy(engine)

        # ── DuckDB ───────────────────────────────────────────────────────────
        if _duckdb and isinstance(engine, _duckdb.DuckDBPyConnection):
            return _schema_via_duckdb(engine)

        # ── SQLite native ────────────────────────────────────────────────────
        if isinstance(engine, sqlite3.Connection):
            return _schema_via_sqlite(engine)

        # ── pyodbc / psycopg2 / pymysql native cursor ────────────────────────
        if hasattr(engine, "cursor"):
            return _schema_via_cursor(engine, dt)

        raise RuntimeError(f"Unknown engine type: {type(engine)}")

    except Exception as e:
        _safe_close(engine)
        raise RuntimeError(f"Schema extraction failed: {e}") from e


def _safe_close(engine):
    try:
        if hasattr(engine, "dispose"):
            engine.dispose()
        elif hasattr(engine, "close"):
            engine.close()
    except Exception:
        pass


def _schema_via_sqlalchemy(engine) -> Dict:
    """Extract schema using SQLAlchemy inspection (works for all SA dialects)."""
    insp   = _sa.inspect(engine)
    result = {}
    for table in insp.get_table_names():
        pk_cols = set(insp.get_pk_constraint(table).get("constrained_columns", []))
        fk_cols = {
            fk["constrained_columns"][0]
            for fk in insp.get_foreign_keys(table)
            if fk["constrained_columns"]
        }
        cols = []
        for col in insp.get_columns(table):
            cols.append({
                "name":    col["name"],
                "type":    str(col["type"]).upper()[:30],
                "pk":      col["name"] in pk_cols,
                "fk":      col["name"] in fk_cols,
                "notnull": not col.get("nullable", True),
            })
        result[table] = cols
    engine.dispose()
    return result


def _schema_via_pymssql(conn) -> Dict:
    """Extract schema via pymssql connection using information_schema."""
    cur = conn.cursor()
    cur.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME"
    )
    tables = [r[0] for r in cur.fetchall()]
    result = {}
    for tbl in tables:
        cur.execute(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, "
            "  CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk,"
            "  CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_fk,"
            "  CASE WHEN c.IS_NULLABLE='NO' THEN 1 ELSE 0 END AS not_null "
            "FROM INFORMATION_SCHEMA.COLUMNS c "
            "LEFT JOIN ("
            "  SELECT ku.TABLE_NAME, ku.COLUMN_NAME "
            "  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku "
            "    ON tc.CONSTRAINT_NAME=ku.CONSTRAINT_NAME "
            "  WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY'"
            ") pk ON c.TABLE_NAME=pk.TABLE_NAME AND c.COLUMN_NAME=pk.COLUMN_NAME "
            "LEFT JOIN ("
            "  SELECT ku.TABLE_NAME, ku.COLUMN_NAME "
            "  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku "
            "    ON tc.CONSTRAINT_NAME=ku.CONSTRAINT_NAME "
            "  WHERE tc.CONSTRAINT_TYPE='FOREIGN KEY'"
            ") fk ON c.TABLE_NAME=fk.TABLE_NAME AND c.COLUMN_NAME=fk.COLUMN_NAME "
            f"WHERE c.TABLE_NAME='{tbl}' "
            "ORDER BY c.ORDINAL_POSITION"
        )
        result[tbl] = [
            {
                "name":    r[0],
                "type":    (r[1] or "TEXT").upper()[:30],
                "pk":      bool(r[2]),
                "fk":      bool(r[3]),
                "notnull": bool(r[4]),
            }
            for r in cur.fetchall()
        ]
    conn.close()
    return result


def _schema_via_duckdb(engine) -> Dict:
    rows = engine.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()
    result = {}
    for (tbl,) in rows:
        col_rows = engine.execute(f"PRAGMA table_info('{tbl}')").fetchall()
        result[tbl] = [
            {"name": r[1], "type": (r[2] or "TEXT").upper(),
             "pk": bool(r[5]), "fk": False, "notnull": bool(r[3])}
            for r in col_rows
        ]
    engine.close()
    return result


def _schema_via_sqlite(conn) -> Dict:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [r[0] for r in cursor.fetchall()]
    result = {}
    for tbl in tables:
        cursor.execute(f"PRAGMA table_info(`{tbl}`)")
        col_rows = cursor.fetchall()
        cursor.execute(f"PRAGMA foreign_key_list(`{tbl}`)")
        fk_cols = {r[3] for r in cursor.fetchall()}
        result[tbl] = [
            {"name": r[1], "type": (r[2] or "TEXT").upper(),
             "pk": bool(r[5]), "fk": r[1] in fk_cols, "notnull": bool(r[3])}
            for r in col_rows
        ]
    conn.close()
    return result


def _schema_via_cursor(engine, dt: str) -> Dict:
    """Generic cursor path for pyodbc / psycopg2 / pymysql."""
    cursor = engine.cursor()

    # MSSQL via pyodbc
    if dt in ("mssql", "sqlserver", "sql_server"):
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME"
        )
        tables = [r[0] for r in cursor.fetchall()]
        result = {}
        for tbl in tables:
            cursor.execute(
                "SELECT c.COLUMN_NAME, c.DATA_TYPE, "
                "  CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END,"
                "  CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END,"
                "  CASE WHEN c.IS_NULLABLE='NO' THEN 1 ELSE 0 END "
                "FROM INFORMATION_SCHEMA.COLUMNS c "
                "LEFT JOIN ("
                "  SELECT ku.TABLE_NAME,ku.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
                "  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME=ku.CONSTRAINT_NAME "
                "  WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY'"
                ") pk ON c.TABLE_NAME=pk.TABLE_NAME AND c.COLUMN_NAME=pk.COLUMN_NAME "
                "LEFT JOIN ("
                "  SELECT ku.TABLE_NAME,ku.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
                "  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME=ku.CONSTRAINT_NAME "
                "  WHERE tc.CONSTRAINT_TYPE='FOREIGN KEY'"
                ") fk ON c.TABLE_NAME=fk.TABLE_NAME AND c.COLUMN_NAME=fk.COLUMN_NAME "
                f"WHERE c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION",
                (tbl,)
            )
            result[tbl] = [
                {"name": r[0], "type": (r[1] or "TEXT").upper()[:30],
                 "pk": bool(r[2]), "fk": bool(r[3]), "notnull": bool(r[4])}
                for r in cursor.fetchall()
            ]
        engine.close()
        return result

    # PostgreSQL / MySQL via information_schema
    try:
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = CURRENT_SCHEMA()"
        )
    except Exception:
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = DATABASE()"
        )
    tables = [r[0] for r in cursor.fetchall()]
    result = {}
    placeholder = "%s"
    for tbl in tables:
        cursor.execute(
            "SELECT column_name, data_type, column_key "
            "FROM information_schema.columns WHERE table_name = " + placeholder,
            (tbl,)
        )
        result[tbl] = [
            {"name": r[0], "type": (r[1] or "TEXT").upper()[:30],
             "pk": r[2] == "PRI", "fk": r[2] == "MUL", "notnull": False}
            for r in cursor.fetchall()
        ]
    engine.close()
    return result


# ── Query execution ───────────────────────────────────────────────────────────

def execute_external_query(
    db_type: str, conn_str: str, sql: str
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Execute a SELECT query. Returns (DataFrame, None) or (None, error_str)."""
    try:
        result_tuple = _open_engine(db_type, conn_str)
        engine = result_tuple[0] if isinstance(result_tuple, tuple) else result_tuple

        # pymssql native connection — must handle separately (no SA)
        if _pymssql and isinstance(engine, _pymssql.Connection):
            cur = engine.cursor(as_dict=True)
            cur.execute(sql)
            rows = cur.fetchall()
            engine.close()
            if not rows:
                return pd.DataFrame(), None
            return pd.DataFrame(rows), None

        # SQLAlchemy engine
        if _sa and hasattr(engine, "connect") and not hasattr(engine, "cursor"):
            with engine.connect() as conn:
                df = pd.read_sql_query(_sa.text(sql), conn)
            engine.dispose()
            return df, None

        # DuckDB
        if _duckdb and isinstance(engine, _duckdb.DuckDBPyConnection):
            df = engine.execute(sql).df()
            engine.close()
            return df, None

        # Native cursor: pyodbc / psycopg2 / pymysql / sqlite3
        df = pd.read_sql_query(sql, engine)
        _safe_close(engine)
        return df, None

    except Exception as e:
        return None, str(e)


# ── Persistence helpers ───────────────────────────────────────────────────────

def save_connection_to_db(
    meta_db_path: str, name: str, db_type: str, conn_str_enc: str
) -> int:
    """Insert or update a connection record. Returns new row id."""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(meta_db_path)
    # Upsert: delete old record with same name first to avoid UNIQUE clash
    conn.execute("DELETE FROM external_connections WHERE name=?", (name,))
    cur = conn.execute(
        "INSERT INTO external_connections (name, db_type, conn_str_enc, created_at) "
        "VALUES (?, ?, ?, ?)",
        (name, db_type, conn_str_enc, now)
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return row_id


def load_connections_from_db(meta_db_path: str) -> List[Dict]:
    """Return all saved connections (conn_str redacted)."""
    try:
        conn = sqlite3.connect(meta_db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, name, db_type, created_at FROM external_connections ORDER BY id"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def delete_connection_from_db(meta_db_path: str, name: str) -> bool:
    """Delete by name. Returns True if a row was deleted."""
    try:
        conn = sqlite3.connect(meta_db_path)
        affected = conn.execute(
            "DELETE FROM external_connections WHERE name=?", (name,)
        ).rowcount
        conn.commit()
        conn.close()
        return affected > 0
    except Exception:
        return False


def get_connection_str(meta_db_path: str, name: str) -> Optional[Tuple[str, str]]:
    """Return (encrypted_conn_str, db_type) for a given name, or None."""
    try:
        conn = sqlite3.connect(meta_db_path)
        row  = conn.execute(
            "SELECT conn_str_enc, db_type FROM external_connections WHERE name=?", (name,)
        ).fetchone()
        conn.close()
        return row  # (conn_str_enc, db_type) or None
    except Exception:
        return None