"""
data_cleaner.py — Data Cleaning & Preprocessing Engine for LinguaSQL

Two phases:
  1. analyze_table()  → detect all issues, return structured report
  2. apply_cleaning() → apply selected operations, return cleaned DataFrame

Supported operations:
  - drop_duplicates       Remove fully duplicate rows
  - fill_nulls_mean       Fill numeric nulls with column mean
  - fill_nulls_median     Fill numeric nulls with column median
  - fill_nulls_mode       Fill nulls with most frequent value
  - fill_nulls_zero       Fill numeric nulls with 0
  - fill_nulls_empty      Fill text nulls with empty string
  - drop_null_rows        Drop rows with ANY null value
  - drop_null_cols        Drop columns with > threshold% nulls
  - strip_whitespace      Strip leading/trailing whitespace from text cols
  - normalize_text        Lowercase all text columns
  - remove_outliers_iqr   Remove rows with IQR outliers in numeric cols
  - fix_numeric_strings   Coerce object columns that are actually numbers
  - standardize_dates     Parse and standardize date-like string columns
"""

import re
import io
import os
import sqlite3
import csv as _csv
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import numpy as np


# ─────────────────────────────────────────────────────────
#  ISSUE DETECTION
# ─────────────────────────────────────────────────────────

def analyze_table(
    df: pd.DataFrame,
    table_name: str = "data",
) -> Dict:
    """
    Scan a DataFrame for data quality issues.

    Returns a structured report:
    {
      "table": str,
      "total_rows": int,
      "total_cols": int,
      "issues": [
        {
          "id": str,           # unique operation key
          "category": str,     # Duplicates | Missing Values | Type Issues | ...
          "severity": str,     # high | medium | low
          "title": str,
          "description": str,
          "affected_rows": int,
          "affected_cols": [str],
          "auto_fix": bool,    # safe to apply without user review
        }
      ],
      "summary": { "high": int, "medium": int, "low": int }
    }
    """
    issues = []
    n_rows, n_cols = df.shape

    # ── 1. Duplicates ──────────────────────────────────────
    dup_count = int(df.duplicated().sum())
    if dup_count > 0:
        issues.append({
            "id":            "drop_duplicates",
            "category":      "Duplicates",
            "severity":      "high",
            "icon":          "🔁",
            "title":         f"{dup_count:,} duplicate rows",
            "description":   f"{dup_count:,} rows are exact copies of another row and add no information.",
            "affected_rows": dup_count,
            "affected_cols": [],
            "auto_fix":      True,
            "op":            "drop_duplicates",
        })

    # ── 2. Missing values ──────────────────────────────────
    null_counts = df.isnull().sum()
    null_cols   = null_counts[null_counts > 0]

    if len(null_cols) > 0:
        total_nulls = int(null_counts.sum())
        # High severity: any col with >50% nulls
        high_null_cols = [c for c in null_cols.index if null_counts[c] / n_rows > 0.5]
        if high_null_cols:
            issues.append({
                "id":            "drop_null_cols",
                "category":      "Missing Values",
                "severity":      "high",
                "icon":          "🕳️",
                "title":         f"{len(high_null_cols)} column(s) mostly empty (>50% null)",
                "description":   f"Columns {', '.join(high_null_cols[:4])} have over 50% missing values and may not be useful.",
                "affected_rows": 0,
                "affected_cols": high_null_cols,
                "auto_fix":      False,
                "op":            "drop_null_cols",
                "params":        {"threshold": 50},
            })

        # Numeric cols with nulls → suggest fill
        num_null_cols = [c for c in null_cols.index
                         if pd.api.types.is_numeric_dtype(df[c])]
        if num_null_cols:
            issues.append({
                "id":            "fill_nulls_mean",
                "category":      "Missing Values",
                "severity":      "medium",
                "icon":          "🔢",
                "title":         f"Fill {int(null_counts[num_null_cols].sum()):,} numeric nulls with mean",
                "description":   f"Columns: {', '.join(num_null_cols[:4])}. Replace missing numbers with the column average.",
                "affected_rows": int(df[num_null_cols].isnull().any(axis=1).sum()),
                "affected_cols": num_null_cols,
                "auto_fix":      True,
                "op":            "fill_nulls_mean",
            })
            issues.append({
                "id":            "fill_nulls_median",
                "category":      "Missing Values",
                "severity":      "medium",
                "icon":          "📊",
                "title":         f"Fill {int(null_counts[num_null_cols].sum()):,} numeric nulls with median",
                "description":   f"Columns: {', '.join(num_null_cols[:4])}. More robust than mean for skewed data.",
                "affected_rows": int(df[num_null_cols].isnull().any(axis=1).sum()),
                "affected_cols": num_null_cols,
                "auto_fix":      True,
                "op":            "fill_nulls_median",
            })

        # Text cols with nulls
        text_null_cols = [c for c in null_cols.index
                          if not pd.api.types.is_numeric_dtype(df[c])]
        if text_null_cols:
            issues.append({
                "id":            "fill_nulls_mode",
                "category":      "Missing Values",
                "severity":      "low",
                "icon":          "🔤",
                "title":         f"Fill {int(null_counts[text_null_cols].sum()):,} text nulls with most frequent value",
                "description":   f"Columns: {', '.join(text_null_cols[:4])}.",
                "affected_rows": int(df[text_null_cols].isnull().any(axis=1).sum()),
                "affected_cols": text_null_cols,
                "auto_fix":      True,
                "op":            "fill_nulls_mode",
            })
            issues.append({
                "id":            "fill_nulls_empty",
                "category":      "Missing Values",
                "severity":      "low",
                "icon":          "📝",
                "title":         f"Fill text nulls with empty string",
                "description":   f"Replace missing text values with '' instead of NULL.",
                "affected_rows": int(df[text_null_cols].isnull().any(axis=1).sum()),
                "affected_cols": text_null_cols,
                "auto_fix":      True,
                "op":            "fill_nulls_empty",
            })

        # Drop rows with any null
        any_null_rows = int(df.isnull().any(axis=1).sum())
        if any_null_rows > 0 and any_null_rows < n_rows * 0.3:
            issues.append({
                "id":            "drop_null_rows",
                "category":      "Missing Values",
                "severity":      "medium",
                "icon":          "🗑️",
                "title":         f"Drop {any_null_rows:,} rows with any missing value",
                "description":   f"{any_null_rows:,} rows have at least one null. Dropping them gives a fully complete dataset.",
                "affected_rows": any_null_rows,
                "affected_cols": list(null_cols.index),
                "auto_fix":      False,
                "op":            "drop_null_rows",
            })

    # ── 3. Whitespace & Text Issues ────────────────────────
    text_cols = df.select_dtypes(include="object").columns.tolist()
    ws_cols = []
    for col in text_cols:
        series = df[col].dropna().astype(str)
        if (series != series.str.strip()).any():
            ws_cols.append(col)
    if ws_cols:
        ws_count = sum(int((df[c].dropna().astype(str) != df[c].dropna().astype(str).str.strip()).sum())
                       for c in ws_cols)
        issues.append({
            "id":            "strip_whitespace",
            "category":      "Text Quality",
            "severity":      "medium",
            "icon":          "✂️",
            "title":         f"Strip whitespace in {len(ws_cols)} column(s)",
            "description":   f"{ws_count:,} values have leading/trailing spaces: {', '.join(ws_cols[:4])}.",
            "affected_rows": ws_count,
            "affected_cols": ws_cols,
            "auto_fix":      True,
            "op":            "strip_whitespace",
        })

    # Case inconsistency (mixed case in same column)
    case_cols = []
    for col in text_cols[:20]:
        series = df[col].dropna().astype(str)
        uniq = series.unique()
        if len(uniq) > 2:
            lower_set = set(s.lower() for s in uniq)
            if len(lower_set) < len(uniq) * 0.8:
                case_cols.append(col)
    if case_cols:
        issues.append({
            "id":            "normalize_text",
            "category":      "Text Quality",
            "severity":      "low",
            "icon":          "🔡",
            "title":         f"Normalize text case in {len(case_cols)} column(s)",
            "description":   f"Columns {', '.join(case_cols[:4])} have mixed case (e.g. 'Male' vs 'male'). Lowercase standardizes grouping.",
            "affected_rows": n_rows,
            "affected_cols": case_cols,
            "auto_fix":      False,
            "op":            "normalize_text",
        })

    # ── 4. Numeric Strings ─────────────────────────────────
    num_str_cols = []
    for col in text_cols:
        series = df[col].dropna().astype(str).str.strip()
        converted = pd.to_numeric(series, errors="coerce")
        pct_numeric = converted.notna().mean()
        if pct_numeric >= 0.85:
            num_str_cols.append(col)
    if num_str_cols:
        issues.append({
            "id":            "fix_numeric_strings",
            "category":      "Type Issues",
            "severity":      "high",
            "icon":          "🔢",
            "title":         f"{len(num_str_cols)} column(s) stored as text but contain numbers",
            "description":   f"Columns {', '.join(num_str_cols[:4])} look numeric but are stored as text — this prevents calculations.",
            "affected_rows": n_rows,
            "affected_cols": num_str_cols,
            "auto_fix":      True,
            "op":            "fix_numeric_strings",
        })

    # ── 5. Outliers (IQR method) ───────────────────────────
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    outlier_cols = []
    total_outliers = 0
    for col in num_cols:
        s = df[col].dropna()
        if len(s) < 10:
            continue
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        outlier_mask = (s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)
        n_out = int(outlier_mask.sum())
        if n_out > 0 and n_out < len(s) * 0.1:
            outlier_cols.append(col)
            total_outliers += n_out
    if outlier_cols:
        issues.append({
            "id":            "remove_outliers_iqr",
            "category":      "Outliers",
            "severity":      "medium",
            "icon":          "📡",
            "title":         f"{total_outliers:,} statistical outliers across {len(outlier_cols)} column(s)",
            "description":   f"Columns {', '.join(outlier_cols[:4])} have values beyond 1.5× IQR. May skew analysis.",
            "affected_rows": total_outliers,
            "affected_cols": outlier_cols,
            "auto_fix":      False,
            "op":            "remove_outliers_iqr",
        })

    # ── 6. Date strings ────────────────────────────────────
    date_cols = []
    DATE_PATTERNS = [
        r'\d{4}-\d{2}-\d{2}',
        r'\d{2}/\d{2}/\d{4}',
        r'\d{2}-\d{2}-\d{4}',
    ]
    for col in text_cols[:20]:
        series = df[col].dropna().astype(str).head(20)
        for pat in DATE_PATTERNS:
            if series.str.match(pat).mean() > 0.7:
                date_cols.append(col)
                break
    if date_cols:
        issues.append({
            "id":            "standardize_dates",
            "category":      "Type Issues",
            "severity":      "low",
            "icon":          "📅",
            "title":         f"Standardize {len(date_cols)} date column(s) to ISO format",
            "description":   f"Columns {', '.join(date_cols[:4])} appear to contain dates. Parsing to YYYY-MM-DD improves sorting and filtering.",
            "affected_rows": n_rows,
            "affected_cols": date_cols,
            "auto_fix":      True,
            "op":            "standardize_dates",
        })

    # ── 7. Constant / near-constant columns ───────────────
    const_cols = [c for c in df.columns if df[c].nunique() <= 1]
    if const_cols:
        issues.append({
            "id":            "drop_constant_cols",
            "category":      "Redundancy",
            "severity":      "low",
            "icon":          "🟰",
            "title":         f"{len(const_cols)} constant column(s) with only 1 unique value",
            "description":   f"Columns {', '.join(const_cols[:4])} carry no information (all values identical).",
            "affected_rows": 0,
            "affected_cols": const_cols,
            "auto_fix":      True,
            "op":            "drop_constant_cols",
        })

    summary = {
        "high":   sum(1 for i in issues if i["severity"] == "high"),
        "medium": sum(1 for i in issues if i["severity"] == "medium"),
        "low":    sum(1 for i in issues if i["severity"] == "low"),
    }

    return {
        "table":      table_name,
        "total_rows": n_rows,
        "total_cols": n_cols,
        "issues":     issues,
        "summary":    summary,
        "clean_pct":  _clean_score(df, n_rows),
    }


def _clean_score(df: pd.DataFrame, n_rows: int) -> int:
    """0-100 data quality score."""
    if n_rows == 0:
        return 100
    null_pct  = df.isnull().mean().mean() * 100
    dup_pct   = df.duplicated().sum() / n_rows * 100
    score     = max(0, 100 - null_pct * 1.5 - dup_pct * 2)
    return round(score)


# ─────────────────────────────────────────────────────────
#  CLEANING OPERATIONS
# ─────────────────────────────────────────────────────────

def apply_cleaning(
    df: pd.DataFrame,
    operations: List[str],
    params:     Dict[str, Any] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Apply a list of cleaning operations to a DataFrame.

    Returns:
        (cleaned_df, log_messages)
    """
    params = params or {}
    log    = []
    orig   = len(df)

    for op in operations:
        try:
            before = len(df)

            if op == "drop_duplicates":
                df = df.drop_duplicates()
                removed = before - len(df)
                if removed:
                    log.append(f"✅ Removed {removed:,} duplicate rows")

            elif op == "fill_nulls_mean":
                num_cols = df.select_dtypes(include=[np.number]).columns
                for col in num_cols:
                    n = int(df[col].isnull().sum())
                    if n:
                        df[col] = df[col].fillna(df[col].mean())
                        log.append(f"✅ Filled {n:,} nulls in '{col}' with mean ({df[col].mean():.2f})")

            elif op == "fill_nulls_median":
                num_cols = df.select_dtypes(include=[np.number]).columns
                for col in num_cols:
                    n = int(df[col].isnull().sum())
                    if n:
                        df[col] = df[col].fillna(df[col].median())
                        log.append(f"✅ Filled {n:,} nulls in '{col}' with median ({df[col].median():.2f})")

            elif op == "fill_nulls_mode":
                text_cols = df.select_dtypes(include="object").columns
                for col in text_cols:
                    n = int(df[col].isnull().sum())
                    if n:
                        mode_val = df[col].mode()
                        if len(mode_val):
                            df[col] = df[col].fillna(mode_val.iloc[0])
                            log.append(f"✅ Filled {n:,} nulls in '{col}' with mode ('{mode_val.iloc[0]}')")

            elif op == "fill_nulls_zero":
                num_cols = df.select_dtypes(include=[np.number]).columns
                for col in num_cols:
                    n = int(df[col].isnull().sum())
                    if n:
                        df[col] = df[col].fillna(0)
                        log.append(f"✅ Filled {n:,} nulls in '{col}' with 0")

            elif op == "fill_nulls_empty":
                text_cols = df.select_dtypes(include="object").columns
                for col in text_cols:
                    n = int(df[col].isnull().sum())
                    if n:
                        df[col] = df[col].fillna("")
                        log.append(f"✅ Filled {n:,} nulls in '{col}' with empty string")

            elif op == "drop_null_rows":
                df = df.dropna()
                removed = before - len(df)
                if removed:
                    log.append(f"✅ Dropped {removed:,} rows with any null value")

            elif op == "drop_null_cols":
                threshold = params.get("drop_null_cols_threshold", 50) / 100
                cols_before = set(df.columns)
                df = df.dropna(axis=1, thresh=int(len(df) * (1 - threshold)))
                dropped = cols_before - set(df.columns)
                if dropped:
                    log.append(f"✅ Dropped {len(dropped)} column(s) with >{int(threshold*100)}% nulls: {', '.join(dropped)}")

            elif op == "strip_whitespace":
                text_cols = df.select_dtypes(include="object").columns
                for col in text_cols:
                    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
                log.append(f"✅ Stripped whitespace from {len(text_cols)} text column(s)")

            elif op == "normalize_text":
                text_cols = df.select_dtypes(include="object").columns
                for col in text_cols:
                    df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
                log.append(f"✅ Lowercased {len(text_cols)} text column(s)")

            elif op == "fix_numeric_strings":
                text_cols = df.select_dtypes(include="object").columns
                fixed = 0
                for col in text_cols:
                    series = df[col].dropna().astype(str).str.strip()
                    converted = pd.to_numeric(series, errors="coerce")
                    if converted.notna().mean() >= 0.85:
                        df[col] = pd.to_numeric(df[col].astype(str).str.strip(), errors="coerce")
                        fixed += 1
                        log.append(f"✅ Converted '{col}' from text to numeric")

            elif op == "remove_outliers_iqr":
                num_cols = df.select_dtypes(include=[np.number]).columns
                mask = pd.Series([True] * len(df), index=df.index)
                for col in num_cols:
                    s = df[col].dropna()
                    q1, q3 = s.quantile(0.25), s.quantile(0.75)
                    iqr = q3 - q1
                    if iqr > 0:
                        mask &= df[col].between(q1 - 1.5 * iqr, q3 + 1.5 * iqr) | df[col].isnull()
                df = df[mask]
                removed = before - len(df)
                if removed:
                    log.append(f"✅ Removed {removed:,} outlier rows (IQR method)")

            elif op == "standardize_dates":
                text_cols = df.select_dtypes(include="object").columns
                for col in text_cols:
                    try:
                        parsed = pd.to_datetime(df[col], infer_datetime_format=True, errors="coerce")
                        if parsed.notna().mean() > 0.7:
                            df[col] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), df[col])
                            log.append(f"✅ Standardized dates in '{col}' to YYYY-MM-DD")
                    except Exception:
                        pass

            elif op == "drop_constant_cols":
                const_cols = [c for c in df.columns if df[c].nunique() <= 1]
                if const_cols:
                    df = df.drop(columns=const_cols)
                    log.append(f"✅ Dropped {len(const_cols)} constant column(s): {', '.join(const_cols)}")

        except Exception as e:
            log.append(f"⚠️ '{op}' skipped: {e}")

    removed_total = orig - len(df)
    if removed_total:
        log.append(f"📊 {orig:,} → {len(df):,} rows ({removed_total:,} removed, {len(df)/orig*100:.1f}% retained)")

    return df, log


# ─────────────────────────────────────────────────────────
#  EXPORT HELPERS
# ─────────────────────────────────────────────────────────

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Return UTF-8 CSV bytes for the DataFrame."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def df_to_cleaned_sqlite(
    df: pd.DataFrame,
    table_name: str,
    db_path: str,
) -> None:
    """Write cleaned DataFrame to a new SQLite file."""
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()


def build_cleaning_pdf(
    table_name: str,
    original_shape: Tuple[int, int],
    cleaned_shape:  Tuple[int, int],
    operations:     List[str],
    log:            List[str],
    issues_applied: List[Dict],
) -> bytes:
    """
    Build a branded PDF cleaning report using ReportLab.
    Returns raw PDF bytes.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer,
            Table, TableStyle, HRFlowable,
        )
        import io as _io

        PAGE_W, PAGE_H = A4
        MARGIN = 20 * mm
        C_BLUE   = colors.HexColor("#2563EB")
        C_VIOLET = colors.HexColor("#7C3AED")
        C_GREEN  = colors.HexColor("#16A34A")
        C_AMBER  = colors.HexColor("#D97706")
        C_RED    = colors.HexColor("#DC2626")
        C_INK    = colors.HexColor("#1A1714")
        C_INK3   = colors.HexColor("#8A837A")
        C_BG     = colors.HexColor("#F7F6F3")
        C_BORDER = colors.HexColor("#E5E0D8")
        WHITE    = colors.white

        def S(name, **kw): return ParagraphStyle(name, **kw)

        st = {
            "h1":    S("h1",  fontName="Helvetica-Bold", fontSize=20, textColor=C_INK, leading=26, spaceAfter=4),
            "h2":    S("h2",  fontName="Helvetica-Bold", fontSize=13, textColor=C_INK, leading=17, spaceAfter=2, spaceBefore=8),
            "body":  S("body",fontName="Helvetica",      fontSize=10, textColor=C_INK, leading=14),
            "small": S("small",fontName="Helvetica",     fontSize=9,  textColor=C_INK3, leading=12),
            "log":   S("log", fontName="Courier",        fontSize=9,  textColor=colors.HexColor("#334155"), leading=13),
        }

        buf = _io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4,
                                leftMargin=MARGIN, rightMargin=MARGIN,
                                topMargin=18*mm,   bottomMargin=18*mm,
                                title=f"Cleaning Report: {table_name}")

        date_str = datetime.now().strftime("%B %d, %Y  %H:%M")

        def on_page(canvas, doc):
            canvas.saveState()
            canvas.setFillColor(C_BLUE)
            canvas.rect(0, PAGE_H - 14*mm, PAGE_W/2, 14*mm, fill=1, stroke=0)
            canvas.setFillColor(C_VIOLET)
            canvas.rect(PAGE_W/2, PAGE_H - 14*mm, PAGE_W/2, 14*mm, fill=1, stroke=0)
            canvas.setFillColor(WHITE)
            canvas.roundRect(MARGIN - 2, PAGE_H - 11*mm, 7*mm, 7*mm, 1.5*mm, fill=1, stroke=0)
            canvas.setFillColor(C_BLUE)
            canvas.setFont("Helvetica-Bold", 8.5)
            canvas.drawCentredString(MARGIN + 1.5*mm, PAGE_H - 7*mm, "Q")
            canvas.setFillColor(WHITE)
            canvas.setFont("Helvetica-Bold", 10)
            canvas.drawString(MARGIN + 9*mm, PAGE_H - 7*mm, "LinguaSQL")
            canvas.setFont("Helvetica", 8)
            canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 7*mm, f"Page {doc.page}")
            canvas.setFillColor(C_BORDER)
            canvas.rect(MARGIN, 10*mm, PAGE_W - 2*MARGIN, 0.3*mm, fill=1, stroke=0)
            canvas.setFillColor(C_INK3)
            canvas.setFont("Helvetica", 7.5)
            canvas.drawString(MARGIN, 7*mm, f"LinguaSQL Data Cleaning Report · {date_str}")
            canvas.drawRightString(PAGE_W - MARGIN, 7*mm, "Confidential")
            canvas.restoreState()

        story = [
            Spacer(1, 4*mm),
            Paragraph(f"🧹 Data Cleaning Report", st["h1"]),
            Paragraph(f"Table: <b>{table_name}</b>   ·   {date_str}", st["small"]),
            HRFlowable(width="100%", thickness=0.5, color=C_BORDER,
                       spaceAfter=4*mm, spaceBefore=4*mm),
        ]

        # Summary stats
        rows_removed = original_shape[0] - cleaned_shape[0]
        cols_removed = original_shape[1] - cleaned_shape[1]
        retain_pct   = cleaned_shape[0] / original_shape[0] * 100 if original_shape[0] else 100
        stats = [
            [Paragraph("<b>Metric</b>", st["body"]),
             Paragraph("<b>Before</b>", st["body"]),
             Paragraph("<b>After</b>", st["body"]),
             Paragraph("<b>Change</b>", st["body"])],
            [Paragraph("Rows", st["body"]),
             Paragraph(f"{original_shape[0]:,}", st["body"]),
             Paragraph(f"{cleaned_shape[0]:,}", st["body"]),
             Paragraph(f"-{rows_removed:,}" if rows_removed else "—", st["body"])],
            [Paragraph("Columns", st["body"]),
             Paragraph(f"{original_shape[1]:,}", st["body"]),
             Paragraph(f"{cleaned_shape[1]:,}", st["body"]),
             Paragraph(f"-{cols_removed:,}" if cols_removed else "—", st["body"])],
            [Paragraph("Data retained", st["body"]),
             Paragraph("100%", st["body"]),
             Paragraph(f"{retain_pct:.1f}%", st["body"]),
             Paragraph(f"{retain_pct-100:.1f}%", st["body"])],
        ]
        stats_tbl = Table(stats, colWidths=[60*mm, 35*mm, 35*mm, 35*mm])
        stats_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,0),  C_BLUE),
            ("TEXTCOLOR",     (0,0),(-1,0),  WHITE),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, C_BG]),
            ("GRID",          (0,0),(-1,-1), 0.4, C_BORDER),
            ("LINEBELOW",     (0,0),(-1,0),  1.5, C_BLUE),
            ("TOPPADDING",    (0,0),(-1,-1), 6),
            ("BOTTOMPADDING", (0,0),(-1,-1), 6),
            ("LEFTPADDING",   (0,0),(-1,-1), 8),
        ]))
        story += [Paragraph("Summary", st["h2"]), stats_tbl, Spacer(1, 4*mm)]

        # Operations applied
        if operations:
            story.append(Paragraph("Operations Applied", st["h2"]))
            op_rows = [[Paragraph("<b>#</b>", st["body"]),
                        Paragraph("<b>Operation</b>", st["body"])]]
            for i, op in enumerate(operations, 1):
                nice = op.replace("_", " ").title()
                op_rows.append([Paragraph(str(i), st["body"]),
                                 Paragraph(nice, st["body"])])
            op_tbl = Table(op_rows, colWidths=[15*mm, 150*mm])
            op_tbl.setStyle(TableStyle([
                ("BACKGROUND",    (0,0),(-1,0),  colors.HexColor("#F0EDE8")),
                ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
                ("GRID",          (0,0),(-1,-1), 0.4, C_BORDER),
                ("TOPPADDING",    (0,0),(-1,-1), 5),
                ("BOTTOMPADDING", (0,0),(-1,-1), 5),
                ("LEFTPADDING",   (0,0),(-1,-1), 6),
            ]))
            story += [op_tbl, Spacer(1, 4*mm)]

        # Change log
        if log:
            story.append(Paragraph("Change Log", st["h2"]))
            for entry in log:
                story.append(Paragraph(entry, st["log"]))
                story.append(Spacer(1, 1*mm))

        doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
        return buf.getvalue()

    except ImportError:
        # Fallback: plain-text PDF via minimal reportlab
        import io as _io
        txt = f"LinguaSQL Data Cleaning Report\n{'='*40}\n"
        txt += f"Table: {table_name}\nDate: {datetime.now()}\n\n"
        txt += f"Rows: {original_shape[0]} → {cleaned_shape[0]}\n"
        txt += f"Cols: {original_shape[1]} → {cleaned_shape[1]}\n\n"
        txt += "Operations:\n" + "\n".join(f"- {op}" for op in operations)
        txt += "\n\nLog:\n" + "\n".join(log)
        # Return as plain bytes (not PDF) if reportlab unavailable
        return txt.encode()
