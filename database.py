"""
╔══════════════════════════════════════════════════════════════╗
║  database.py  —  All database operations for LinguaSQL       ║
╚══════════════════════════════════════════════════════════════╝

This module is the FOUNDATION of the project.
Everything else (server, AI, importer) calls functions from here.

WHAT IT DOES:
  1. Maintains a registry of all available databases (display name → file path)
  2. Reads database schemas (tables, columns, types, keys)
  3. Converts schemas to text for the AI prompt
  4. Executes SQL queries safely and returns Pandas DataFrames
  5. Validates SQL (only SELECT is allowed — safety!)
  6. Provides stats, previews, and utility functions

WHY SQLITE?
  SQLite stores an entire database in a single .db file.
  No separate server needed. Python has it built-in (import sqlite3).
  Perfect for a portable student project.
"""

import os
import sqlite3
from typing import Dict, List, Tuple, Optional
import pandas as pd

# External DB support (PostgreSQL / MySQL / MSSQL / DuckDB)
try:
    from external_db import (
        is_external, get_external_schema, execute_external_query,
        _EXTERNAL_CONNECTIONS, EXTERNAL_MARKER,
    )
    _EXTERNAL_SUPPORT = True
except ImportError:
    _EXTERNAL_SUPPORT = False
    def is_external(n): return False

# Optional DuckDB engine for large CSV / Parquet files
try:
    import duckdb as _duckdb_engine
    DUCKDB_AVAILABLE = True
except ImportError:
    _duckdb_engine   = None
    DUCKDB_AVAILABLE = False


# ─────────────────────────────────────────────────────────
#  ENGINE METADATA REGISTRY
#  Tracks which engine (sqlite / duckdb) each database uses.
#  Format: db_name → {"engine": "sqlite"|"duckdb", "source_path": str}
# ─────────────────────────────────────────────────────────

_DB_ENGINE_META: Dict[str, Dict] = {}


def set_db_engine(db_name: str, engine: str, source_path: str = "") -> None:
    """Record the query engine for a registered database."""
    _DB_ENGINE_META[db_name] = {"engine": engine, "source_path": source_path}


def get_db_engine(db_name: str) -> str:
    """Return 'duckdb' or 'sqlite' for the given database name."""
    return _DB_ENGINE_META.get(db_name, {}).get("engine", "sqlite")


def get_db_source_path(db_name: str) -> str:
    """For DuckDB databases return the original file path (CSV/Parquet)."""
    return _DB_ENGINE_META.get(db_name, {}).get("source_path", "")


# ─────────────────────────────────────────────────────────
#  DATABASE REGISTRY
#  Maps human-friendly display names → file paths.
#  This is the single source of truth for all databases.
# ─────────────────────────────────────────────────────────

DATABASE_REGISTRY: Dict[str, str] = {
    "🎓 College Database":    "databases/college.db",
    "🛒 E-Commerce Database": "databases/ecommerce.db",
    "🏥 Hospital Database":   "databases/hospital.db",
}

# Names of the built-in (non-deletable) databases
_BUILTIN_NAMES = frozenset(DATABASE_REGISTRY.keys())


def register_database(display_name: str, db_path: str) -> None:
    """Add a new database to the registry (called after file import)."""
    DATABASE_REGISTRY[display_name] = db_path


def unregister_database(display_name: str) -> None:
    """Remove a database from the registry (called on delete)."""
    DATABASE_REGISTRY.pop(display_name, None)


def is_builtin_database(name: str) -> bool:
    """Returns True for the 3 sample databases (they cannot be deleted)."""
    return name in _BUILTIN_NAMES


def get_all_database_names() -> List[str]:
    """Return all registered database names for the UI dropdown."""
    return list(DATABASE_REGISTRY.keys())


def _get_path(db_name: str) -> str:
    """
    Resolve display name → file path or external marker.

    Tries multiple lookup strategies so that emoji encoding differences,
    URL-decode artifacts, browser normalization, and partial name matches
    all still resolve correctly.
    """
    if not db_name:
        raise FileNotFoundError("No database selected. Please pick a database from the dropdown.")

    # 1. Exact match — fastest path
    if db_name in DATABASE_REGISTRY:
        return DATABASE_REGISTRY[db_name]

    # 2. Strip ALL leading emoji/icon/space chars and try again
    import unicodedata
    def _strip_prefix(s: str) -> str:
        """Remove leading emoji, symbols, and spaces."""
        i = 0
        for ch in s:
            cat = unicodedata.category(ch)
            if cat.startswith('S') or cat in ('Zs', 'Ps', 'Pe') or ch in ' \t':
                i += 1
            else:
                break
        return s[i:]

    bare = _strip_prefix(db_name).strip()

    # 3. Try all known emoji prefixes with the bare name
    PREFIXES = ["🔌 ", "⚡ ", "📂 ", "🦆 ", "🐘 ", "🐬 ", "🏢 ", "🎓 ", "🛒 ", "🏥 ", ""]
    for pfx in PREFIXES:
        candidate = pfx + bare
        if candidate in DATABASE_REGISTRY:
            return DATABASE_REGISTRY[candidate]

    # 4. Case-insensitive full match
    db_lower = db_name.lower().strip()
    for key, val in DATABASE_REGISTRY.items():
        if key.lower().strip() == db_lower:
            return val

    # 5. Case-insensitive bare-name match (ignore prefix differences)
    bare_lower = bare.lower()
    for key, val in DATABASE_REGISTRY.items():
        key_bare = _strip_prefix(key).strip().lower()
        if key_bare == bare_lower:
            return val

    # 6. SQL Server instance name: strip the server\instance prefix
    #    e.g. "MYPC\SQLEXPRESS" → try matching "SQLEXPRESS" or "MYPC"
    if '\\' in bare or '/' in bare:
        parts = bare.replace('\\', '/').split('/')
        for part in parts:
            part_l = part.strip().lower()
            for key, val in DATABASE_REGISTRY.items():
                key_bare_l = _strip_prefix(key).strip().lower()
                if part_l and (part_l in key_bare_l or key_bare_l in part_l):
                    return val

    # 7. Partial / contains match as last resort
    for key, val in DATABASE_REGISTRY.items():
        key_bare_l = _strip_prefix(key).strip().lower()
        if bare_lower and (bare_lower in key_bare_l or key_bare_l in bare_lower):
            return val

    # 8. Any word overlap match (handles "BHARTISINGHSQLEXPRESS" vs "SQLEXPRESS")
    bare_words = set(bare_lower.replace('_', ' ').split())
    for key, val in DATABASE_REGISTRY.items():
        key_words = set(_strip_prefix(key).strip().lower().replace('_', ' ').split())
        if bare_words & key_words:  # any common word
            return val

    available = list(DATABASE_REGISTRY.keys())
    raise FileNotFoundError(
        f"Database '{db_name}' not found in registry. "
        f"Available databases: {available}. "
        f"If you connected an external database, try restarting the server — "
        f"connections are reloaded from the database on startup."
    )


# ─────────────────────────────────────────────────────────
#  SCHEMA READING
# ─────────────────────────────────────────────────────────

def _get_schema_duckdb(db_name: str) -> Dict[str, List[Dict]]:
    """Extract column metadata from a DuckDB-backed source file."""
    src = get_db_source_path(db_name)
    if not src or not DUCKDB_AVAILABLE:
        return {}
    try:
        ext = os.path.splitext(src)[1].lower()
        table_name = os.path.splitext(os.path.basename(src))[0]
        con = _duckdb_engine.connect(":memory:")

        if ext == ".parquet":
            con.execute(f"CREATE VIEW \"{table_name}\" AS SELECT * FROM read_parquet('{src}')")
        elif ext in (".csv", ".tsv", ".txt"):
            sep = "\t" if ext == ".tsv" else ","
            con.execute(
                f"CREATE VIEW \"{table_name}\" AS "
                f"SELECT * FROM read_csv_auto('{src}', delim='{sep}')"
            )
        else:
            con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
            table_name = None  # multi-table duckdb file handled below

        # DESCRIBE gives column names and types
        if table_name:
            rows = con.execute(f"DESCRIBE \"{table_name}\"").fetchall()
            cols = [{"name": r[0], "type": str(r[1]).upper()[:30],
                     "pk": False, "fk": False, "notnull": False}
                    for r in rows]
            con.close()
            return {table_name: cols}

        # .duckdb file: list all tables
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        result = {}
        for tbl in tables:
            rows = con.execute(f"DESCRIBE {tbl}").fetchall()
            result[tbl] = [{"name": r[0], "type": str(r[1]).upper()[:30],
                            "pk": False, "fk": False, "notnull": False}
                           for r in rows]
        con.close()
        return result
    except Exception as e:
        return {"error_reading_schema": [{"name": str(e), "type": "ERROR", "pk": False, "fk": False, "notnull": False}]}


def get_schema(db_name: str) -> Dict[str, List[Dict]]:
    """
    Read the database and return its structure.
    Routes to DuckDB / external_db / SQLite as appropriate.
    """
    # DuckDB engine path — read column info from the source file
    if get_db_engine(db_name) == "duckdb":
        return _get_schema_duckdb(db_name)

    # External DB path
    if _EXTERNAL_SUPPORT and is_external(db_name):
        meta = _EXTERNAL_CONNECTIONS[db_name]
        return get_external_schema(meta["db_type"], meta["conn_str"])

    # SQLite path (original behaviour)
    path = _get_path(db_name)
    conn = sqlite3.connect(path)
    result = {}

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            cursor.execute(f"PRAGMA table_info(`{table}`)")
            col_rows = cursor.fetchall()
            cursor.execute(f"PRAGMA foreign_key_list(`{table}`)")
            fk_from_cols = {row[3] for row in cursor.fetchall()}
            result[table] = [
                {
                    "name": r[1],
                    "type": (r[2] or "TEXT").upper(),
                    "pk":   bool(r[5]),
                    "fk":   r[1] in fk_from_cols,
                    "notnull": bool(r[3]),
                }
                for r in col_rows
            ]
    finally:
        conn.close()

    return result


def schema_to_text(db_name: str) -> str:
    """
    Convert the schema dict to a plain-text string for the AI prompt.

    WHY THIS FORMAT?
      The AI reads this text and uses it to write correct SQL.
      It needs to know exact table names, column names, and types.
      The [PRIMARY KEY] and [FK] flags help the AI write better JOINs.

    Example output:
        Table: students
          - id (INTEGER) [PRIMARY KEY]
          - name (TEXT)
          - cgpa (REAL)
          - dept_id (INTEGER) [FK]

        Table: departments
          - id (INTEGER) [PRIMARY KEY]
          - name (TEXT)
    """
    schema = get_schema(db_name)
    lines = []

    for table, columns in schema.items():
        lines.append(f"Table: {table}")
        for col in columns:
            flags = []
            if col["pk"]:  flags.append("PRIMARY KEY")
            if col["fk"]:  flags.append("FK")
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            lines.append(f"  - {col['name']} ({col['type']}){flag_str}")
        lines.append("")  # blank line between tables

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────
#  SQL EXECUTION
# ─────────────────────────────────────────────────────────

def execute_query_duckdb(source_path: str, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Run a SQL query using DuckDB against a CSV, Parquet, or .duckdb file.

    DuckDB can query CSV and Parquet files DIRECTLY without loading them
    into memory first — far faster than SQLite for files >10 MB.

    DuckDB auto-registers CSV/Parquet as a virtual table named after the
    file stem, so the AI-generated SQL (SELECT * FROM sales_data) just works.
    """
    if not DUCKDB_AVAILABLE:
        return None, (
            "DuckDB is not installed. Run: pip install duckdb\n"
            "Alternatively, re-import this file without the DuckDB option."
        )
    try:
        con = _duckdb_engine.connect(":memory:")

        ext = os.path.splitext(source_path)[1].lower()
        if ext == ".parquet":
            # DuckDB reads Parquet natively: SELECT * FROM 'file.parquet'
            # Replace table references in the SQL with quoted file path
            table_name = os.path.splitext(os.path.basename(source_path))[0]
            # Register as a view so existing SQL table names work
            con.execute(f"CREATE VIEW \"{table_name}\" AS SELECT * FROM read_parquet('{source_path}')")
        elif ext in (".csv", ".tsv", ".txt"):
            table_name = os.path.splitext(os.path.basename(source_path))[0]
            sep = "\t" if ext == ".tsv" else ","
            con.execute(
                f"CREATE VIEW \"{table_name}\" AS "
                f"SELECT * FROM read_csv_auto('{source_path}', delim='{sep}')"
            )
        else:
            # .duckdb file — attach directly
            con.execute(f"ATTACH '{source_path}' AS src (READ_ONLY)")
            con.execute("USE src")

        rel = con.execute(sql)
        df  = rel.df()
        con.close()
        return df, None
    except Exception as e:
        return None, str(e)


def execute_query(db_name: str, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute a SQL query and return (DataFrame, error_message).
    Routes to the correct engine:
      - DuckDB  → execute_query_duckdb()  (for large CSV/Parquet imports)
      - External → execute_external_query() (for network DB connections)
      - SQLite  → pd.read_sql_query()     (default)
    """
    # DuckDB engine path
    if get_db_engine(db_name) == "duckdb":
        src = get_db_source_path(db_name)
        return execute_query_duckdb(src, sql)

    # External DB path
    if _EXTERNAL_SUPPORT and is_external(db_name):
        meta = _EXTERNAL_CONNECTIONS[db_name]
        return execute_external_query(meta["db_type"], meta["conn_str"], sql)

    # SQLite path
    path = _get_path(db_name)
    try:
        conn = sqlite3.connect(path)
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)


def validate_sql(sql: str) -> Tuple[bool, str]:
    """
    Safety check — only SELECT queries are allowed.

    WHY THIS MATTERS:
      Without validation, a user (or a misbehaving AI) could send:
        DROP TABLE students;      ← deletes all student data!
        DELETE FROM students;     ← same!
        UPDATE students SET cgpa=10; ← corrupts data!

      We use a whitelist approach:
        1. Query must START with SELECT
        2. Query must NOT contain dangerous keywords

    Returns:
        (True, "OK")                   — safe to execute
        (False, "reason it failed")    — blocked
    """
    DANGEROUS = [
        "DROP", "DELETE", "UPDATE", "INSERT",
        "ALTER", "CREATE", "TRUNCATE", "REPLACE",
        "ATTACH", "DETACH", "PRAGMA",
    ]

    cleaned = sql.strip()
    if not cleaned:
        return False, "Query is empty"

    upper = cleaned.upper()

    # Must start with SELECT (or WITH for CTEs)
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        return False, "Only SELECT queries are allowed"

    # Must not contain any dangerous keyword
    for kw in DANGEROUS:
        # Check for keyword as a whole word (not part of a column name)
        import re
        if re.search(r'\b' + kw + r'\b', upper):
            return False, f"Keyword '{kw}' is not allowed"

    return True, "OK"


# ─────────────────────────────────────────────────────────
#  UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────

def get_database_stats(db_name: str) -> Dict[str, int]:
    """
    Returns row counts for every table in the database.
    Works for both SQLite and external databases.
    """
    schema = get_schema(db_name)
    stats  = {}

    # For external DBs query each table individually
    if _EXTERNAL_SUPPORT and is_external(db_name):
        for table in schema:
            try:
                df, _ = execute_query(db_name, f'SELECT COUNT(*) AS n FROM "{table}"')
                stats[table] = int(df.iloc[0, 0]) if df is not None and not df.empty else 0
            except Exception:
                stats[table] = 0
        return stats

    # SQLite path
    path = _get_path(db_name)
    conn = sqlite3.connect(path)
    try:
        for table in schema:
            try:
                cur = conn.execute(f"SELECT COUNT(*) FROM `{table}`")
                stats[table] = cur.fetchone()[0]
            except Exception:
                stats[table] = 0
    finally:
        conn.close()
    return stats


def get_table_preview(db_name: str, table_name: str, limit: int = 5) -> pd.DataFrame:
    """
    Returns the first `limit` rows of a table as a DataFrame.
    Used for the table preview feature in the import tab.
    """
    df, error = execute_query(db_name, f"SELECT * FROM `{table_name}` LIMIT {limit}")
    if df is not None:
        return df
    return pd.DataFrame()


def reload_uploaded_databases() -> int:
    """
    Scans the databases/uploads/ folder for .db files and
    registers any that aren't already in the registry.

    Called on server startup to restore previously uploaded databases
    that were registered in a previous session.

    Returns: number of databases reloaded
    """
    upload_dir = "databases/uploads"
    if not os.path.exists(upload_dir):
        return 0

    count = 0
    for fname in sorted(os.listdir(upload_dir)):
        if fname.endswith(".db"):
            display_name = "📄 " + fname[:-3]  # Remove .db extension
            db_path = os.path.join(upload_dir, fname)
            if display_name not in DATABASE_REGISTRY:
                register_database(display_name, db_path)
                count += 1

    return count
