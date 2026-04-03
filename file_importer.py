"""
╔══════════════════════════════════════════════════════════════╗
║  file_importer.py  —  Upload any file → LinguaSQL database   ║
╚══════════════════════════════════════════════════════════════╝

Converts uploaded files into SQLite databases that LinguaSQL can query.

SUPPORTED FORMATS:
  .csv, .tsv, .txt  → pandas.read_csv()
  .xlsx, .xls       → pandas.read_excel()
  .json             → pandas.read_json()
  .html, .htm       → pandas.read_html() — extracts <table> elements
  .parquet          → pandas.read_parquet()
  .db, .sqlite,
  .sqlite3          → copied directly (already SQLite)

THE IMPORT PIPELINE (same for every format):
  1. DETECT FORMAT  — file extension → correct reader
  2. READ FILE      — format-specific Pandas reader → DataFrame(s)
  3. CLEAN DATA     — fix column names, infer types
  4. WRITE DATABASE — DataFrame(s) → SQLite .db file
  5. REGISTER       — add to DATABASE_REGISTRY → appears in UI

WHY PANDAS?
  Pandas handles all the complex parsing (CSV quoting, Excel merged
  cells, JSON nesting) and gives us a uniform DataFrame regardless
  of input format. Writing DataFrame → SQLite is then just one call:
  df.to_sql("table_name", conn, if_exists="replace", index=False)
"""

import os
import re
import io
import shutil
import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

from database import register_database, unregister_database, DATABASE_REGISTRY, DUCKDB_AVAILABLE, set_db_engine


# ─────────────────────────────────────────────────────────
#  SUPPORTED FILE EXTENSIONS
# ─────────────────────────────────────────────────────────

SUPPORTED_EXTENSIONS = {
    ".csv":     "CSV (Comma-Separated Values)",
    ".tsv":     "TSV (Tab-Separated Values)",
    ".txt":     "Plain Text (CSV format)",
    ".xlsx":    "Excel 2007+ Workbook",
    ".xls":     "Excel 97-2003 Workbook",
    ".json":    "JSON (JavaScript Object Notation)",
    ".html":    "HTML Table",
    ".htm":     "HTML Table",
    ".parquet": "Apache Parquet",
    ".db":      "SQLite Database",
    ".sqlite":  "SQLite Database",
    ".sqlite3": "SQLite Database",
}


# ─────────────────────────────────────────────────────────
#  COLUMN NAME CLEANER
# ─────────────────────────────────────────────────────────

def clean_column_name(name: str) -> str:
    """
    Convert any column name into a valid SQLite column name.

    SQLite column names cannot have spaces or most special characters.
    This function sanitises them consistently.

    Examples:
        "First Name"       → "first_name"
        "AGE (Years)"      → "age_years"
        "% Score"          → "score"
        "2024 Sales"       → "col_2024_sales"
        "  "               → "column"
        "user-id"          → "user_id"
    """
    name = str(name).strip().lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)   # Replace non-alphanumeric with _
    name = re.sub(r'_+', '_', name)             # Collapse consecutive underscores
    name = name.strip('_')                       # Remove leading/trailing underscores

    if not name:
        return "column"

    # Column names cannot start with a number
    if name[0].isdigit():
        name = "col_" + name

    # Avoid SQLite reserved words
    RESERVED = {"select", "from", "where", "table", "index", "order", "group",
                "join", "on", "as", "and", "or", "not", "null", "is", "in",
                "by", "desc", "asc", "limit", "offset", "having", "with"}
    if name in RESERVED:
        name = f"{name}_col"

    return name


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a DataFrame for SQLite import:
      1. Rename columns to valid SQL names
      2. Drop completely empty rows
      3. Make column names unique if duplicates exist after cleaning
    """
    # Rename columns
    seen = {}
    new_cols = []
    for col in df.columns:
        clean = clean_column_name(str(col))
        if clean in seen:
            seen[clean] += 1
            clean = f"{clean}_{seen[clean]}"
        else:
            seen[clean] = 0
        new_cols.append(clean)
    df.columns = new_cols

    # Drop rows where ALL values are NaN
    df = df.dropna(how='all')

    # Replace numpy NaN with None (SQLite NULL)
    df = df.where(pd.notnull(df), None)

    return df


# ─────────────────────────────────────────────────────────
#  FORMAT-SPECIFIC READERS
#  Each returns Dict[table_name → DataFrame]
# ─────────────────────────────────────────────────────────

def read_csv_file(file_bytes: bytes, filename: str) -> Dict[str, pd.DataFrame]:
    """Read CSV / TSV / TXT → single DataFrame."""
    ext = os.path.splitext(filename)[1].lower()
    sep = "\t" if ext == ".tsv" else ","

    # Try different encodings — files from Windows often use cp1252
    for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            df = pd.read_csv(
                io.BytesIO(file_bytes),
                sep=sep,
                encoding=encoding,
                on_bad_lines='skip',   # skip malformed rows
                engine='python',
            )
            table_name = clean_column_name(os.path.splitext(filename)[0]) or "data"
            return {table_name: clean_dataframe(df)}
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    raise ValueError(f"Could not read '{filename}'. Try saving as UTF-8 CSV.")


def read_excel_file(file_bytes: bytes, filename: str) -> Dict[str, pd.DataFrame]:
    """
    Read Excel (.xlsx / .xls) → one DataFrame per sheet.

    Multi-sheet workbooks get one database table per sheet.
    Sheet names become table names after cleaning.
    """
    base_name = clean_column_name(os.path.splitext(filename)[0]) or "sheet"
    all_sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    result = {}
    for sheet_name, df in all_sheets.items():
        if df.empty:
            continue
        table_name = clean_column_name(sheet_name) or base_name
        result[table_name] = clean_dataframe(df)
    if not result:
        raise ValueError("Excel file contains no data")
    return result


def read_json_file(file_bytes: bytes, filename: str) -> Dict[str, pd.DataFrame]:
    """
    Read JSON → DataFrame.

    Supports these JSON structures:
      - Array of objects: [{"name":"x","age":25}, ...]
      - Single object: {"name":"x","age":25}
      - Nested: tries to flatten with json_normalize
    """
    import json
    table_name = clean_column_name(os.path.splitext(filename)[0]) or "data"

    try:
        data = json.loads(file_bytes.decode("utf-8"))
    except UnicodeDecodeError:
        data = json.loads(file_bytes.decode("latin-1"))

    if isinstance(data, list):
        df = pd.json_normalize(data)
    elif isinstance(data, dict):
        # Could be {"data": [...]} or a flat object
        for key, value in data.items():
            if isinstance(value, list) and len(value) > 0:
                df = pd.json_normalize(value)
                break
        else:
            df = pd.json_normalize([data])
    else:
        raise ValueError("JSON must contain an array or object")

    return {table_name: clean_dataframe(df)}


def read_html_file(file_bytes: bytes, filename: str) -> Dict[str, pd.DataFrame]:
    """
    Read HTML → extract <table> elements → one DataFrame per table.
    """
    base_name = clean_column_name(os.path.splitext(filename)[0]) or "table"
    tables = pd.read_html(io.BytesIO(file_bytes))

    if not tables:
        raise ValueError("No <table> elements found in HTML file")

    result = {}
    for i, df in enumerate(tables):
        if df.empty:
            continue
        name = base_name if i == 0 else f"{base_name}_{i+1}"
        result[name] = clean_dataframe(df)
    return result


def read_parquet_file(file_bytes: bytes, filename: str) -> Dict[str, pd.DataFrame]:
    """Read Apache Parquet format → DataFrame."""
    table_name = clean_column_name(os.path.splitext(filename)[0]) or "data"
    df = pd.read_parquet(io.BytesIO(file_bytes))
    return {table_name: clean_dataframe(df)}


def import_sqlite_file(file_bytes: bytes, filename: str) -> dict:
    """
    Handle direct SQLite database uploads.
    Instead of converting, we save and register directly.
    """
    base_name  = clean_column_name(os.path.splitext(filename)[0]) or "uploaded"
    db_path    = f"databases/uploads/{base_name}.db"
    os.makedirs("databases/uploads", exist_ok=True)

    with open(db_path, "wb") as f:
        f.write(file_bytes)

    # Verify it is a valid SQLite file
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
        conn.close()
    except Exception as e:
        os.remove(db_path)
        raise ValueError(f"Invalid SQLite file: {e}")

    display_name = f"📄 {base_name}"
    register_database(display_name, db_path)

    return {
        "success":  True,
        "db_name":  display_name,
        "db_path":  db_path,
        "tables":   tables,
        "message":  f"SQLite database imported with {len(tables)} table(s)",
    }


# ─────────────────────────────────────────────────────────
#  WRITE DATAFRAMES TO SQLITE
# ─────────────────────────────────────────────────────────

def dataframes_to_sqlite(
    dfs: Dict[str, pd.DataFrame],
    db_path: str,
) -> None:
    """
    Write a dict of DataFrames to a SQLite database file.

    Uses df.to_sql() which:
      - Creates the table with appropriate SQLite types
      - Handles NULL values (NaN → NULL)
      - Overwrites existing tables (if_exists="replace")
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        for table_name, df in dfs.items():
            df.to_sql(
                name       = table_name,
                con        = conn,
                if_exists  = "replace",
                index      = False,
                chunksize  = 1000,     # Batch inserts for large files
            )
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────
#  MASTER IMPORT FUNCTION  (called by server.py)
# ─────────────────────────────────────────────────────────

_DUCKDB_ELIGIBLE_EXTS  = {".csv", ".tsv", ".txt", ".parquet"}
_DUCKDB_SIZE_THRESHOLD = 10 * 1024 * 1024   # 10 MB


def import_file_duckdb(file_bytes: bytes, filename: str) -> dict:
    """
    Import a CSV or Parquet file using DuckDB as the query engine.

    Instead of converting to SQLite, the raw file is saved to disk and
    DuckDB queries it directly at query time — far faster for large files
    because it uses columnar storage and vectorised execution.
    """
    if not DUCKDB_AVAILABLE:
        return {"success": False,
                "error": "DuckDB is not installed. Run: pip install duckdb"}

    ext       = os.path.splitext(filename)[1].lower()
    base_name = clean_column_name(os.path.splitext(filename)[0]) or "uploaded"
    os.makedirs("databases/uploads", exist_ok=True)
    src_path  = f"databases/uploads/{base_name}{ext}"

    # Save the raw file to disk (DuckDB reads it in-place)
    with open(src_path, "wb") as f:
        f.write(file_bytes)

    # Validate: quick schema read + row count
    try:
        import duckdb
        con = duckdb.connect(":memory:")
        if ext == ".parquet":
            con.execute(f"CREATE VIEW \"{base_name}\" AS SELECT * FROM read_parquet('{src_path}')")
        else:
            sep = "\t" if ext == ".tsv" else ","
            con.execute(
                f"CREATE VIEW \"{base_name}\" AS "
                f"SELECT * FROM read_csv_auto('{src_path}', delim='{sep}')"
            )
        rows_result = con.execute(f"SELECT COUNT(*) FROM \"{base_name}\"").fetchone()
        total_rows  = rows_result[0] if rows_result else 0
        con.close()
    except Exception as e:
        if os.path.exists(src_path):
            os.remove(src_path)
        return {"success": False, "error": f"DuckDB validation failed: {e}"}

    # Register with DuckDB engine marker
    display_name = f"⚡ {base_name}"
    register_database(display_name, src_path)
    set_db_engine(display_name, "duckdb", src_path)

    return {
        "success":     True,
        "db_name":     display_name,
        "db_path":     src_path,
        "tables":      [base_name],
        "message":     f"⚡ DuckDB import — {total_rows:,} rows (columnar engine)",
        "engine":      "duckdb",
        "source_path": src_path,
    }


def import_file(file_bytes: bytes, filename: str,
                use_duckdb: bool = False) -> dict:
    """
    Import any supported file and register it as a queryable database.

    Args:
        file_bytes:  Raw bytes of the uploaded file
        filename:    Original filename (used to detect format and name the DB)
        use_duckdb:  If True and file is CSV/Parquet, use DuckDB engine.
                     Auto-suggested for files > 10 MB.

    Returns dict with keys: success, db_name, db_path, tables, message,
    and optionally: engine, source_path, duckdb_available.
    """
    try:
        ext = os.path.splitext(filename)[1].lower()

        # DuckDB path — for eligible types when requested
        if use_duckdb and ext in _DUCKDB_ELIGIBLE_EXTS:
            return import_file_duckdb(file_bytes, filename)

        # Route to format-specific reader
        if ext in (".db", ".sqlite", ".sqlite3"):
            return import_sqlite_file(file_bytes, filename)
        elif ext in (".csv", ".tsv", ".txt"):
            dfs = read_csv_file(file_bytes, filename)
        elif ext in (".xlsx", ".xls"):
            dfs = read_excel_file(file_bytes, filename)
        elif ext == ".json":
            dfs = read_json_file(file_bytes, filename)
        elif ext in (".html", ".htm"):
            dfs = read_html_file(file_bytes, filename)
        elif ext == ".parquet":
            dfs = read_parquet_file(file_bytes, filename)
        else:
            return {"success": False, "error": f"Unsupported file type: '{ext}'"}

        # Build the output database path
        base_name  = clean_column_name(os.path.splitext(filename)[0]) or "uploaded"
        db_path    = f"databases/uploads/{base_name}.db"

        # Write all DataFrames to the SQLite file
        dataframes_to_sqlite(dfs, db_path)

        # Register in the database registry
        display_name = f"📄 {base_name}"
        register_database(display_name, db_path)
        set_db_engine(display_name, "sqlite", db_path)

        total_rows = sum(len(df) for df in dfs.values())
        tables     = list(dfs.keys())

        # Suggest DuckDB if file is large and DuckDB is available
        suggest_duckdb = (
            DUCKDB_AVAILABLE
            and len(file_bytes) >= _DUCKDB_SIZE_THRESHOLD
            and ext in _DUCKDB_ELIGIBLE_EXTS
        )

        return {
            "success":          True,
            "db_name":          display_name,
            "db_path":          db_path,
            "tables":           tables,
            "message":          f"Imported {len(tables)} table(s) — {total_rows:,} rows total",
            "engine":           "sqlite",
            "suggest_duckdb":   suggest_duckdb,
            "duckdb_available": DUCKDB_AVAILABLE,
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


def delete_uploaded_database(db_path: str) -> bool:
    """Delete a database file from disk. Returns True on success."""
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
        return True
    except Exception:
        return False
