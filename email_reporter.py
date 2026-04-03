"""
email_reporter.py — Scheduled email report engine for LinguaSQL

Handles:
  • Cron expression parsing (daily/weekly/monthly + custom 5-field cron)
  • HTML email template rendering (branded, with data table + insights)
  • SMTP delivery via smtplib (reads SMTP_* env vars)
  • Background scheduler thread (checks due reports every 60 s)
  • AES-256-CBC encryption of stored API keys via Fernet (falls back to
    base64 obfuscation if cryptography package is unavailable)

No third-party scheduler libraries required — pure stdlib + optional cryptography.
"""

import os
import re
import json
import time
import sqlite3
import smtplib
import threading
import traceback
from base64 import b64encode, b64decode
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict, Optional, Tuple

# ── Optional: strong encryption for stored API keys ──────────────────────────
try:
    from cryptography.fernet import Fernet
    import hashlib, os as _os

    def _fernet() -> Fernet:
        secret = os.environ.get("QM_SECRET_KEY", "linguasql-default-secret-key-32!!")
        key = hashlib.sha256(secret.encode()).digest()
        return Fernet(b64encode(key))

    def encrypt_key(plain: str) -> str:
        if not plain:
            return ""
        return _fernet().encrypt(plain.encode()).decode()

    def decrypt_key(token: str) -> str:
        if not token:
            return ""
        try:
            return _fernet().decrypt(token.encode()).decode()
        except Exception:
            return ""

    CRYPTO_AVAILABLE = True

except ImportError:
    # Fallback: simple base64 obfuscation (not secure — tells users to set QM_SECRET_KEY)
    def encrypt_key(plain: str) -> str:
        return b64encode(plain.encode()).decode() if plain else ""

    def decrypt_key(token: str) -> str:
        try:
            return b64decode(token.encode()).decode() if token else ""
        except Exception:
            return ""

    CRYPTO_AVAILABLE = False


# ─────────────────────────────────────────────────────────
#  CRON HELPERS (stdlib only)
# ─────────────────────────────────────────────────────────

PRESET_CRONS = {
    "daily":   "0 8 * * *",          # 08:00 every day
    "weekly":  "0 8 * * 1",          # 08:00 every Monday
    "monthly": "0 8 1 * *",          # 08:00 1st of month
    "hourly":  "0 * * * *",          # top of every hour
}


def _cron_next_run(cron: str, after: Optional[datetime] = None) -> datetime:
    """
    Compute the next datetime a 5-field cron expression fires after `after`
    (defaults to now). Supports * and exact integers for each field.
    Fields: minute hour day_of_month month day_of_week
    """
    after = after or datetime.now()
    parts = cron.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron: '{cron}' — expected 5 fields")

    def _match(val: int, field: str) -> bool:
        if field == "*":
            return True
        try:
            return int(field) == val
        except ValueError:
            # Handle */n step syntax
            if field.startswith("*/"):
                step = int(field[2:])
                return val % step == 0
            return False

    minute, hour, dom, month, dow = parts
    candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)

    # Search up to 4 years (prevents infinite loop on bad expressions)
    for _ in range(2 * 365 * 24 * 60):
        if (
            _match(candidate.minute,   minute) and
            _match(candidate.hour,     hour)   and
            _match(candidate.day,      dom)    and
            _match(candidate.month,    month)  and
            _match(candidate.weekday() if dow != "*" else 0, dow)
        ):
            return candidate
        candidate += timedelta(minutes=1)

    raise ValueError(f"Could not compute next run for cron: '{cron}'")


def cron_for_preset(preset: str, hour: int = 8, minute: int = 0,
                     weekday: int = 1) -> str:
    """
    Build a cron string from a human-friendly preset.
    preset: 'daily' | 'weekly' | 'monthly' | 'hourly' | raw cron string
    """
    if preset == "daily":
        return f"{minute} {hour} * * *"
    if preset == "weekly":
        return f"{minute} {hour} * * {weekday}"
    if preset == "monthly":
        return f"{minute} {hour} 1 * *"
    if preset == "hourly":
        return f"0 * * * *"
    # Treat as raw cron; validate it parses
    _cron_next_run(preset)
    return preset


def human_readable_cron(cron: str) -> str:
    """Return a short human description of a cron string."""
    parts = cron.strip().split()
    if len(parts) != 5:
        return cron
    minute, hour, dom, month, dow = parts
    days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

    # Hourly: "M * * * *"
    if hour == "*" and dom == "*" and month == "*" and dow == "*":
        try:
            return f"Every hour at :{int(minute):02d}"
        except ValueError:
            return "Every hour"

    try:
        t = f"{int(hour):02d}:{int(minute):02d}"
    except ValueError:
        t = f"{hour}:{minute}"

    if dom == "*" and month == "*":
        if dow == "*":
            return f"Daily at {t}"
        try:
            return f"Every {days[int(dow)]} at {t}"
        except (ValueError, IndexError):
            return f"Weekly (day {dow}) at {t}"
    if dom != "*" and month == "*":
        return f"Monthly on day {dom} at {t}"
    return cron


# ─────────────────────────────────────────────────────────
#  HTML EMAIL TEMPLATE
# ─────────────────────────────────────────────────────────

def build_html_email(
    report_name:  str,
    question:     str,
    db_name:      str,
    sql:          str,
    columns:      List[str],
    rows:         List[Dict],
    insights:     List[Dict],
    run_time:     str,
    next_run:     str,
    total_rows:   int,
) -> str:
    """Render a fully self-contained HTML email with inlined CSS.
    Shows: summary stats banner, AI insights, full data table, footer.
    SQL is never shown (hidden from email and PDF).
    """

    max_table_rows = 50
    display_rows   = rows[:max_table_rows]
    truncated      = total_rows > max_table_rows
    has_data       = bool(columns and display_rows)

    # ── Summary stats banner ──────────────────────────────
    # Auto-detect numeric columns for quick stats
    numeric_stats = []
    if has_data:
        for col in columns[:6]:
            vals = []
            for r in rows:
                v = r.get(col)
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    pass
            if vals and len(vals) >= len(rows) * 0.5:  # >50% numeric
                total = sum(vals)
                avg   = total / len(vals)
                numeric_stats.append({
                    "col":   col,
                    "total": total,
                    "avg":   avg,
                    "count": len(vals),
                })
                if len(numeric_stats) >= 4:
                    break

    stat_cards_html = ""
    if numeric_stats:
        def fmt_num(n):
            if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
            if n >= 1_000:     return f"{n/1_000:.1f}K"
            if n == int(n):    return f"{int(n):,}"
            return f"{n:.2f}"

        for s in numeric_stats:
            stat_cards_html += f"""
            <td style="text-align:center;padding:0 12px">
              <div style="font-size:22px;font-weight:800;color:#2563EB;line-height:1.2">{fmt_num(s['total'])}</div>
              <div style="font-size:10px;color:#6B7280;text-transform:uppercase;letter-spacing:.6px;margin-top:2px">Total {_he(s['col'])}</div>
            </td>"""

    # Always show row count
    stat_cards_html = f"""
        <td style="text-align:center;padding:0 12px">
          <div style="font-size:22px;font-weight:800;color:#2563EB;line-height:1.2">{total_rows:,}</div>
          <div style="font-size:10px;color:#6B7280;text-transform:uppercase;letter-spacing:.6px;margin-top:2px">Total Rows</div>
        </td>
        <td style="text-align:center;padding:0 12px">
          <div style="font-size:22px;font-weight:800;color:#7C3AED;line-height:1.2">{len(columns)}</div>
          <div style="font-size:10px;color:#6B7280;text-transform:uppercase;letter-spacing:.6px;margin-top:2px">Columns</div>
        </td>""" + stat_cards_html

    stats_section = f"""
    <div style="margin-bottom:24px">
      <table style="width:100%;background:#F8FAFC;border:1px solid #E5E7EB;
                    border-radius:10px;border-collapse:collapse">
        <tr style="border-top:3px solid #2563EB">
          {stat_cards_html}
        </tr>
        <tr><td colspan="10" style="padding:10px 16px 0">
          <div style="font-size:12px;color:#9CA3AF">
            📁 Database: <strong style="color:#374151">{_he(db_name)}</strong>
            &nbsp;·&nbsp; ⏱ Generated: <strong style="color:#374151">{_he(run_time)}</strong>
            &nbsp;·&nbsp; 🔄 Next run: <strong style="color:#374151">{_he(next_run)}</strong>
          </div>
        </td></tr>
        <tr><td colspan="10" style="height:12px"></td></tr>
      </table>
    </div>
    """

    # ── AI Insights ───────────────────────────────────────
    ICON_BG = {
        "📈":"#F0FDF4","📉":"#FEF2F2","🏆":"#FFFBEB","⚠️":"#FFFBEB",
        "💡":"#EFF6FF","🔍":"#F5F3FF","📊":"#EFF6FF","🎯":"#F0FDF4",
        "⚡":"#FFFBEB","🔗":"#F5F3FF",
    }
    insight_cards = ""
    for ins in insights:
        icon    = ins.get("icon", "💡")
        text    = ins.get("text", "")
        card_bg = ICON_BG.get(icon, "#EFF6FF")
        insight_cards += f"""
        <tr>
          <td style="padding:10px 8px;vertical-align:top;font-size:18px;
                     background:{card_bg};border-radius:6px 0 0 6px;width:40px;
                     text-align:center">{icon}</td>
          <td style="padding:10px 14px;font-size:13px;color:#374151;
                     line-height:1.6;background:{card_bg};
                     border-radius:0 6px 6px 0">{_he(text)}</td>
        </tr>
        <tr><td colspan="2" style="height:6px;background:transparent"></td></tr>"""

    insights_section = f"""
    <div style="margin-bottom:28px">
      <h3 style="font-size:12px;font-weight:700;color:#374151;
                 text-transform:uppercase;letter-spacing:.8px;
                 margin:0 0 12px;display:flex;align-items:center">
        <span style="display:inline-block;width:3px;height:14px;
                     background:#7C3AED;border-radius:2px;margin-right:8px"></span>
        AI Insights
      </h3>
      <table style="width:100%;border-collapse:separate;border-spacing:0">
        {insight_cards}
      </table>
    </div>
    """ if insights else ""

    # ── Data table ────────────────────────────────────────
    th_cells = "".join(
        f'<th style="background:#1E3A5F;color:#fff;padding:10px 14px;'
        f'text-align:left;font-size:11px;font-weight:600;white-space:nowrap;'
        f'border-right:1px solid #2563EB;letter-spacing:.3px">{_he(c)}</th>'
        for c in columns
    )
    tr_rows = ""
    for i, row in enumerate(display_rows):
        bg = "#FFFFFF" if i % 2 == 0 else "#F7F8FC"
        border_b = "#E5E7EB" if i < len(display_rows) - 1 else "transparent"
        cells = "".join(
            f'<td style="padding:9px 14px;font-size:12px;color:#374151;'
            f'border-bottom:1px solid {border_b};white-space:nowrap;max-width:200px;'
            f'overflow:hidden;text-overflow:ellipsis">'
            f'{_he(str(row.get(c, "")) if row.get(c) is not None else "—")}</td>'
            for c in columns
        )
        tr_rows += f'<tr style="background:{bg}">{cells}</tr>'

    truncation_note = (
        f'<p style="font-size:11px;color:#9CA3AF;margin:8px 0 0;text-align:right;'
        f'font-style:italic">Showing first {max_table_rows} of {total_rows:,} rows. '
        f'See attached PDF for context.</p>'
        if truncated else ""
    )

    table_section = f"""
    <div style="margin-bottom:28px">
      <h3 style="font-size:12px;font-weight:700;color:#374151;
                 text-transform:uppercase;letter-spacing:.8px;margin:0 0 12px">
        <span style="display:inline-block;width:3px;height:14px;
                     background:#2563EB;border-radius:2px;margin-right:8px"></span>
        Data Results
        <span style="font-weight:400;color:#9CA3AF;font-size:11px;
                     text-transform:none;letter-spacing:0">
          &nbsp;({total_rows:,} rows returned)
        </span>
      </h3>
      <div style="overflow-x:auto;border-radius:8px;border:1px solid #E5E7EB;
                  box-shadow:0 1px 4px rgba(0,0,0,.04)">
        <table style="width:100%;border-collapse:collapse;min-width:320px">
          <thead><tr>{th_cells}</tr></thead>
          <tbody>{tr_rows}</tbody>
        </table>
      </div>
      {truncation_note}
    </div>
    """ if has_data else f"""
    <div style="margin-bottom:28px;padding:20px;background:#FEF2F2;
                border-radius:8px;border:1px solid #FECACA">
      <p style="margin:0;font-size:13px;color:#991B1B">
        ⚠️ The report query returned no rows for the question:<br>
        <em style="color:#7F1D1D">"{_he(question)}"</em>
      </p>
    </div>
    """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_he(report_name)}</title>
</head>
<body style="margin:0;padding:0;background:#F3F4F6;
             font-family:'Helvetica Neue',Helvetica,Arial,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#F3F4F6;padding:32px 0">
<tr><td align="center">
<table width="640" cellpadding="0" cellspacing="0"
       style="background:#ffffff;border-radius:14px;overflow:hidden;
              box-shadow:0 4px 24px rgba(0,0,0,.09)">

  <!-- Gradient header -->
  <tr>
    <td style="background:linear-gradient(135deg,#1D4ED8 0%,#7C3AED 100%);
               padding:22px 32px">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td>
            <span style="background:#fff;color:#2563EB;font-weight:800;
                         font-size:13px;border-radius:6px;padding:3px 10px;
                         letter-spacing:-.3px">Q</span>
            <span style="color:#fff;font-size:17px;font-weight:700;
                         margin-left:9px;vertical-align:middle;
                         letter-spacing:-.3px">LinguaSQL</span>
          </td>
          <td align="right">
            <span style="color:rgba(255,255,255,.65);font-size:11px">
              📅 Scheduled Report
            </span>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Report title block -->
  <tr>
    <td style="padding:28px 32px 16px">
      <p style="margin:0 0 4px;font-size:11px;font-weight:600;
                letter-spacing:.9px;text-transform:uppercase;color:#9CA3AF">
        Automated Data Report
      </p>
      <h1 style="margin:0 0 10px;font-size:24px;font-weight:800;
                 color:#111827;letter-spacing:-.4px">
        {_he(report_name)}
      </h1>
      <p style="margin:0;font-size:13px;color:#6B7280;line-height:1.6;
                background:#F8FAFC;padding:10px 14px;border-radius:8px;
                border-left:3px solid #2563EB">
        <strong style="color:#374151">Question:</strong> {_he(question)}
      </p>
    </td>
  </tr>

  <!-- Divider -->
  <tr><td style="padding:0 32px">
    <div style="height:1px;background:#E5E7EB"></div>
  </td></tr>

  <!-- Main content -->
  <tr><td style="padding:24px 32px">
    {stats_section}
    {insights_section}
    {table_section}
  </td></tr>

  <!-- PDF attachment note -->
  <tr>
    <td style="padding:0 32px 20px">
      <div style="background:#EFF6FF;border:1px solid #BFDBFE;border-radius:8px;
                  padding:12px 16px;font-size:12px;color:#1D4ED8">
        📎 <strong>PDF Report attached</strong> — contains the full data table
        ({total_rows:,} rows), AI insights, and report metadata.
        Open the attachment for a print-ready version.
      </div>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="background:#F9FAFB;padding:16px 32px;
               border-top:1px solid #E5E7EB;border-radius:0 0 14px 14px">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="font-size:11px;color:#9CA3AF">
            Generated by <strong style="color:#6B7280">LinguaSQL</strong>
            · {_he(run_time)}
          </td>
          <td align="right" style="font-size:11px;color:#9CA3AF">
            Next report: {_he(next_run)}
          </td>
        </tr>
      </table>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def _he(s: str) -> str:
    """HTML-escape a string."""
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


# ─────────────────────────────────────────────────────────
#  SMTP SENDER
# ─────────────────────────────────────────────────────────

def send_email_report(
    to_email:    str,
    subject:     str,
    html_body:   str,
    pdf_bytes:   Optional[bytes] = None,
    pdf_filename: str = "LinguaSQL_Report.pdf",
) -> Tuple[bool, str]:
    """
    Send an HTML email via SMTP with an optional PDF attachment.
    The PDF contains the full data table + AI insights — NO SQL query shown.

    Reads config from env vars:
      SMTP_HOST     (default: localhost)
      SMTP_PORT     (default: 587)
      SMTP_USER     (default: "")
      SMTP_PASSWORD (default: "")
      SMTP_FROM     (default: SMTP_USER or "linguasql@localhost")
      SMTP_TLS      (default: "true")  — set "false" to use plain SMTP
    Returns (success, error_message).
    """
    from email.mime.base import MIMEBase
    from email import encoders as email_encoders

    host     = os.environ.get("SMTP_HOST",     "localhost")
    port     = int(os.environ.get("SMTP_PORT",  "587"))
    user     = os.environ.get("SMTP_USER",     "")
    password = os.environ.get("SMTP_PASSWORD", "")
    from_    = os.environ.get("SMTP_FROM",     user or "linguasql@localhost")
    use_tls  = os.environ.get("SMTP_TLS", "true").lower() != "false"

    # Use "mixed" so we can attach the PDF
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = from_
    msg["To"]      = to_email

    # HTML body goes in an "alternative" sub-part
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    # Attach PDF if provided
    if pdf_bytes:
        pdf_part = MIMEBase("application", "pdf")
        pdf_part.set_payload(pdf_bytes)
        email_encoders.encode_base64(pdf_part)
        pdf_part.add_header(
            "Content-Disposition",
            "attachment",
            filename=pdf_filename,
        )
        msg.attach(pdf_part)

    try:
        if use_tls:
            server = smtplib.SMTP(host, port, timeout=15)
            server.ehlo()
            server.starttls()
            server.ehlo()
        else:
            server = smtplib.SMTP_SSL(host, port, timeout=15)
        if user and password:
            server.login(user, password)
        server.sendmail(from_, [to_email], msg.as_string())
        server.quit()
        return True, ""
    except Exception as e:
        return False, str(e)


def get_smtp_configured() -> bool:
    """Return True if SMTP_HOST is set (basic check)."""
    return bool(os.environ.get("SMTP_HOST"))


# ─────────────────────────────────────────────────────────
#  SCHEDULER
# ─────────────────────────────────────────────────────────

class ReportScheduler:
    """
    Lightweight background thread that checks for due reports every 60 s.
    No APScheduler dependency — uses threading.Thread + time.sleep.
    """

    def __init__(self, meta_db_path: str, run_report_fn):
        """
        meta_db_path  : path to linguasql_meta.db
        run_report_fn : callable(report_dict) → (success, message)
        """
        self._db_path    = meta_db_path
        self._run_report = run_report_fn
        self._thread: Optional[threading.Thread] = None
        self._stop_evt   = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="qm-scheduler"
        )
        self._thread.start()
        print("📅 Report scheduler started")

    def stop(self):
        self._stop_evt.set()

    def _loop(self):
        while not self._stop_evt.is_set():
            try:
                self._tick()
            except Exception as e:
                print(f"Scheduler tick error: {e}")
            self._stop_evt.wait(60)   # check every minute

    def _tick(self):
        """Fire any reports whose next_run is in the past."""
        now = datetime.now()
        try:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            due = conn.execute(
                "SELECT * FROM scheduled_reports WHERE active=1 AND next_run <= ?",
                (now.strftime("%Y-%m-%d %H:%M:%S"),)
            ).fetchall()
            conn.close()
        except Exception:
            return

        for row in due:
            report = dict(row)
            print(f"📧 Running scheduled report: {report['name']}")
            try:
                ok, msg = self._run_report(report)
                status = "ok" if ok else f"error: {msg}"
            except Exception as e:
                status = f"exception: {e}"
                print(traceback.format_exc())

            # Compute next run and update record
            try:
                next_run = _cron_next_run(report["schedule_cron"])
                conn = sqlite3.connect(self._db_path)
                conn.execute(
                    "UPDATE scheduled_reports SET last_run=?, next_run=?, last_status=? WHERE id=?",
                    (now.strftime("%Y-%m-%d %H:%M:%S"),
                     next_run.strftime("%Y-%m-%d %H:%M:%S"),
                     status, report["id"])
                )
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Scheduler update error: {e}")


# Module-level singleton — created and started by server.py
scheduler: Optional[ReportScheduler] = None
