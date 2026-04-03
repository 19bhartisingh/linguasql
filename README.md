<div align="center">

<img src="https://img.shields.io/badge/LinguaSQL-v12-2563EB?style=for-the-badge&logoColor=white" alt="Version">
<img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
<img src="https://img.shields.io/badge/FastAPI-0.111+-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI">
<img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License">

# 🧠 LinguaSQL

### **Natural Language → SQL Intelligence Platform**

*Ask your database anything in plain English. No SQL required.*

[✨ Features](#-features) · [🚀 Quick Start](#-quick-start) · [⚙️ Configuration](#️-configuration) · [📡 API Reference](#-api-reference) · [🤝 Contributing](#-contributing)

</div>

---

## 🌟 What is LinguaSQL?

LinguaSQL is a **full-stack, AI-powered web application** that converts plain English questions into SQL queries, executes them, and presents results as interactive charts with AI-generated insights — all in real time.

```
User: "Which department has the most students and what's the average CGPA?"
  ↓
LinguaSQL: SELECT d.name, COUNT(s.id) AS students, ROUND(AVG(s.cgpa), 2) AS avg_cgpa
           FROM students s JOIN departments d ON s.dept_id = d.id
           GROUP BY d.name ORDER BY students DESC LIMIT 100
  ↓
→ Bar chart  →  Data table  →  AI insights  →  Step-by-step explanation
```

No SQL knowledge needed. Works with **SQLite, PostgreSQL, MySQL, MS SQL Server, DuckDB**, and local files (CSV, Excel, Parquet, JSON).

---

## ✨ Features

### 🔍 Core Query Engine
| Feature | Description |
|---------|-------------|
| **Natural Language → SQL** | Converts plain English to accurate SQL using LLMs |
| **Conversational Memory** | 5-turn context so follow-ups like *"show only the top 5"* work |
| **Confidence Retry** | Auto-retries with enhanced prompt when AI confidence < 60% |
| **LRU Cache** | 200-entry cache returns identical queries instantly (⚡ badge) |
| **Voice Input** | Web Speech API — query hands-free |
| **Share Links** | UUID-based shareable result links |

### 📊 Visualisation
| Feature | Description |
|---------|-------------|
| **8 Chart Types** | Bar, H-Bar, Line, Area, Pie, Donut, Scatter, Funnel — auto-selected |
| **10 Colour Themes** | Blue, Teal, Violet, Amber, Rose, Emerald, Orange, Cyan, Indigo, Pink |
| **Pivot Table** | Drag-and-drop aggregation on any result set |
| **Chart Builder** | Visual SQL builder: pick table + axes + aggregation + chart type |
| **Text to Dashboard** | One goal → 4-6 coordinated KPI cards + charts |
| **PDF Export** | Branded PDF for query results and full dashboards |

### 🤖 Intelligence Layer *(Unique Features)*
> These features don't exist in any other open-source Text-to-SQL tool.

| Feature | Description |
|---------|-------------|
| **🧠 SQL Reasoning Trace** | AI shows chain-of-thought *before* writing SQL — which tables, which JOINs, why |
| **🔔 Watchdog Alerts** | *"Alert me when daily orders fall below 50"* → AI writes the SQL → runs on schedule → emails you |
| **🧬 Query DNA Graph** | Interactive force-layout graph of table relationships, sized by query frequency |
| **🔍 Schema Detective** | AI detects your database industry and auto-generates relevant example questions |

### 🧹 Data Management
| Feature | Description |
|---------|-------------|
| **Data Cleaner** | Detects 9 issue types (duplicates, nulls, outliers, type errors) — one click, saves back to source |
| **Correlation Matrix** | Pearson heatmap with ECharts + highlight chips for notable pairs |
| **Data Profiling** | Per-column null %, unique count, min/max/mean, top-5 values |
| **Data Dictionary** | AI-generated documentation for every table and column, downloadable as Markdown |
| **DuckDB Engine** | Columnar query engine for large CSV/Parquet (>10 MB) — no conversion needed |
| **External Connections** | Connect PostgreSQL, MySQL, MS SQL, DuckDB via connection strings |

### 👥 Auth & Reports
| Feature | Description |
|---------|-------------|
| **JWT Authentication** | Register/login, per-user query history, 30-day tokens |
| **Scheduled Email Reports** | Daily/Weekly/Monthly AI-generated reports via SMTP |
| **Sidebar Toggle** | Collapse/expand with `Ctrl+B` / `Cmd+B`, state persisted |

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- One AI provider API key (Gemini is free)

### 1. Clone & Install
```bash
git clone https://github.com/yourusername/linguasql.git
cd linguasql
pip install -r requirements.txt
```

### 2. Configure
```bash
cp .env.example .env
```
Edit `.env` and add at least **one** AI provider key:
```env
GEMINI_API_KEY=AIza...        # Free → aistudio.google.com
GROQ_API_KEY=gsk_...          # Free → console.groq.com
OPENAI_API_KEY=sk-...         # Paid → platform.openai.com
```

### 3. Run
```bash
python server.py
```

Open **http://localhost:8000** — three sample databases (College, E-Commerce, Hospital) are created automatically on first run.

> **That's it.** No Docker, no npm, no webpack, no build step.

---

## ⚙️ Configuration

### Environment Variables

```env
# ── AI Providers (at least one required) ──────────────────
GEMINI_API_KEY=AIza...          # Free — https://aistudio.google.com
GROQ_API_KEY=gsk_...            # Free — https://console.groq.com
OPENAI_API_KEY=sk-...           # Paid — https://platform.openai.com
# Ollama: install locally from https://ollama.ai (no key needed)

# ── Security (strongly recommended for production) ─────────
QM_SECRET_KEY=...               # Encrypts stored API keys (32 chars random)
QM_JWT_SECRET=...               # Signs JWT tokens (64 hex chars random)
QM_JWT_EXPIRE_DAYS=30           # Token expiry in days (default: 30)

# ── Email / Scheduled Reports (optional) ──────────────────
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=your_app_password  # Gmail: App Password, not main password
SMTP_FROM=QueryMind <you@gmail.com>

# ── Server ─────────────────────────────────────────────────
PORT=8000
```

**Generate secure keys:**
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"   # QM_SECRET_KEY
python -c "import secrets; print(secrets.token_hex(32))"       # QM_JWT_SECRET
```

### Optional Dependencies

```bash
pip install duckdb            # Faster queries on large CSV/Parquet files
pip install psycopg2-binary   # PostgreSQL connections
pip install pymysql           # MySQL / MariaDB connections
pip install pyodbc            # MS SQL Server connections
```

### Local AI (Fully Offline)
```bash
# Install Ollama from https://ollama.ai
ollama pull qwen2.5-coder:7b
ollama serve
# Select "Ollama" in the provider dropdown — no API key needed
```

---

## 📁 Project Structure

```
querymind/
├── server.py              # FastAPI backend — 40+ REST endpoints (1,700 lines)
├── database.py            # DB registry, schema extraction, query routing
├── nl_to_sql.py           # AI engine — NL→SQL, insights, reasoning, docs (1,400 lines)
├── file_importer.py       # CSV/Excel/JSON/Parquet/SQLite import pipeline
├── external_db.py         # PostgreSQL/MySQL/MSSQL/DuckDB connection layer
├── data_cleaner.py        # 9-type issue detection and cleaning engine
├── auth.py                # JWT auth, PBKDF2 password hashing, user CRUD
├── email_reporter.py      # Background scheduler, SMTP mailer, HTML emails
├── pdf_exporter.py        # ReportLab branded PDF builder
├── sample_databases.py    # Creates 3 built-in sample databases on first run
├── static/
│   └── index.html         # Entire frontend — HTML + CSS + JS (~6,300 lines)
├── databases/             # SQLite files (auto-created at runtime)
│   ├── college.db
│   ├── ecommerce.db
│   ├── hospital.db
│   └── querymind_meta.db  # History, users, alerts, docs, connections
├── requirements.txt
├── .env.example
└── README.md
```

---

## 📡 API Reference

### Authentication
```
POST /api/auth/register     Body: {name, email, password}    → {token, user}
POST /api/auth/login        Body: {email, password}           → {token, user}
GET  /api/auth/me           Header: Authorization: Bearer...  → {user}
PUT  /api/auth/me           Body: {name?, password?}          → {token, user}
```

### Queries
```
POST   /api/query               Natural language → SQL → execute → results
POST   /api/execute             Raw SQL execution
GET    /api/history             Query history (scoped per user if authenticated)
DELETE /api/history             Clear history
```

### Databases & Files
```
GET    /api/databases           List all registered databases
GET    /api/schema/{db}         Full schema with row counts
POST   /api/import              Upload file (multipart, use_duckdb flag supported)
POST   /api/connect             Save external DB connection string
GET    /api/connections         List saved connections
DELETE /api/connections/{name}  Remove connection
```

### Intelligence Layer
```
POST /api/reasoning-trace       AI chain-of-thought before SQL generation
POST /api/schema-detective      Detect database industry + generate smart questions
GET  /api/lineage/{db}          Query DNA: table graph nodes/edges + query history
POST /api/watchdog              Create data alert
GET  /api/watchdog              List all alerts with status
POST /api/watchdog/{id}/run     Run alert check immediately
PATCH /api/watchdog/{id}/toggle Pause or resume an alert
DELETE /api/watchdog/{id}       Delete alert
```

### Data Tools
```
GET  /api/profile/{db}              Column statistics, null rates, top values
GET  /api/correlate/{db}/{table}    Pearson correlation matrix
POST /api/clean/analyze             Detect data quality issues
POST /api/clean/apply               Apply cleaning operations + write back to DB
POST /api/clean/export/csv          Download cleaned dataset as CSV
POST /api/clean/export/pdf          Download branded cleaning report PDF
POST /api/docs/generate             Generate AI data dictionary
GET  /api/docs/{db}                 Retrieve saved documentation
GET  /api/docs/{db}/markdown        Download dictionary as .md file
```

### Reports & Export
```
POST /api/reports/schedule          Create scheduled email report
GET  /api/reports/schedule          List all scheduled reports
DELETE /api/reports/schedule/{id}   Delete report
PATCH /api/reports/schedule/{id}/toggle  Pause/resume
POST /api/reports/run/{id}          Run report now
POST /api/export-pdf                Export query result as PDF
POST /api/export-dashboard-pdf      Export dashboard as PDF
POST /api/share                     Create shareable result link
GET  /api/share/{uuid}              Retrieve shared result
```

---

## 🗂️ Navigation Tabs

| Tab | Description |
|-----|-------------|
| **Query** | Natural language → SQL, chart, AI insights, SQL reasoning trace |
| **Dashboard** | Plain English goal → 4-6 coordinated KPI cards + charts |
| **Profiling** | Column statistics, null rates, Pearson correlation matrix |
| **History** | Persistent query log with one-click re-run |
| **Docs** | AI data dictionary with Markdown download |
| **Charts** | Visual chart builder with live SQL preview |
| **Clean** | Automated data quality detection + one-click cleaning |
| **Watchdog** | Natural language data alerts with email notification |
| **DNA** | Interactive table relationship graph + query history heatmap |

---

## ⌨️ Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Enter` | Send query |
| `Ctrl` / `Cmd` + `B` | Toggle sidebar collapse |
| `Escape` | Close any open modal |

---

## 🔒 Security Notes

- **SQL Injection** — SELECT-only whitelist blocks all writes. Dangerous keywords (`DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `CREATE`, `TRUNCATE`, `ATTACH`, `PRAGMA`) are blocklisted.
- **Passwords** — PBKDF2-HMAC-SHA256, 260,000 iterations, unique 16-byte random salt per user.
- **Timing attacks** — `hmac.compare_digest()` used for constant-time password comparison.
- **Encryption** — API keys and DB connection strings encrypted at rest with Fernet AES-256.
- **JWT** — HS256 signed, tamper-proof. Stateless — no server-side session storage.
- **File uploads** — Hard-capped at 200 MB to prevent denial-of-service.

---

## 🛠️ Tech Stack

| | Technology |
|-|-----------|
| **Backend** | Python 3.10+, FastAPI, Uvicorn |
| **Data** | Pandas, NumPy, SQLAlchemy |
| **Databases** | SQLite (stdlib), DuckDB, PostgreSQL, MySQL, MS SQL |
| **AI** | Google Gemini, OpenAI GPT, Groq LLaMA, Ollama |
| **Charts** | Apache ECharts 5.4.3 |
| **PDF** | ReportLab |
| **Auth** | Custom HS256 JWT (stdlib, no PyJWT dependency) |
| **Encryption** | Fernet (cryptography library) |
| **Frontend** | Vanilla HTML/CSS/JS — single file, zero build tools |

---

## 🗺️ Roadmap

- [ ] SQL Dialect Translator (SQLite ↔ PostgreSQL ↔ BigQuery)
- [ ] Multi-Database Federation (cross-DB JOINs via pandas merge)
- [ ] Data Story Generator (AI-written narrative reports with charts)
- [ ] Query Version History (rollback table state like Git)
- [ ] Semantic Query Search (vector embeddings over query history)
- [ ] Team Workspaces (shared query libraries, access control)
- [ ] SQL Learning Mode (interactive tutor with practice questions)
- [ ] Docker / Docker Compose support

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

**Guidelines:**
- Keep the zero-build philosophy — no npm/webpack in the frontend
- Add a test for any new backend module
- Follow existing patterns: type hints, docstrings, error handling

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgements

- [Spider Dataset](https://yale-lily.github.io/spider) — NL2SQL benchmark that inspired this project
- [Apache ECharts](https://echarts.apache.org) — Excellent charting library powering all visualisations
- [FastAPI](https://fastapi.tiangolo.com) — The backbone of the entire backend
- [Google Gemini](https://ai.google.dev) — Default free AI provider
- [Groq](https://console.groq.com) — Blazing-fast LLaMA inference

---

<div align="center">

**Made with ❤️ using Python, FastAPI, and Apache ECharts**

⭐ Star this repo if you found it useful!

</div>
